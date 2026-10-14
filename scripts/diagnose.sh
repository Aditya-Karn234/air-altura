#!/usr/bin/env bash
#
# Air Altura diagnostics — checks Docker, the app containers, MySQL, Redis and RabbitMQ,
# and scans recent logs for errors. Everything runs through `docker compose exec`, so no
# database clients are needed on the host.
#
# Usage:
#   scripts/diagnose.sh                 run every check
#   scripts/diagnose.sh --since 1h      how far back to scan logs for errors (default 15m)
#   scripts/diagnose.sh --trace <id>    print every log line for one request ID, across services
#   scripts/diagnose.sh --help
#
# Environment overrides:
#   GATEWAY_URL, FLIGHTS_URL, BOOKING_URL, FRONTEND_URL   (default: localhost ports from docker-compose.yml)
#   QUEUE_BACKLOG_WARN     ready messages before a queue is flagged      (default 100)
#   STALE_BOOKING_MINUTES  age of an unpaid booking the cron should have cancelled (default 20)
#
# Exit code: 0 when nothing failed (warnings allowed), 1 when any check failed.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

SINCE="15m"
TRACE_ID=""

GATEWAY_URL="${GATEWAY_URL:-http://localhost:5000}"
FLIGHTS_URL="${FLIGHTS_URL:-http://localhost:3000}"
BOOKING_URL="${BOOKING_URL:-http://localhost:4000}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:5173}"
QUEUE_BACKLOG_WARN="${QUEUE_BACKLOG_WARN:-100}"
STALE_BOOKING_MINUTES="${STALE_BOOKING_MINUTES:-20}"

APP_SERVICES="api-gateway booking-service flights-service notifications-service"
ALL_SERVICES="mysql redis rabbitmq flights-seeder flights-service booking-service api-gateway notifications-service frontend"

usage() {
    sed -n '3,19p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --since) SINCE="${2:?--since needs a value, e.g. 30m}"; shift 2 ;;
        --trace) TRACE_ID="${2:?--trace needs a request ID}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
    esac
done

# ---- Output -------------------------------------------------------------------------

if [ -t 1 ]; then
    RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; DIM=$'\033[2m'; RESET=$'\033[0m'
else
    RED=""; GREEN=""; YELLOW=""; BOLD=""; DIM=""; RESET=""
fi

PASSED=0; WARNED=0; FAILED=0

pass()    { PASSED=$((PASSED + 1)); printf '  %sPASS%s  %s\n' "$GREEN" "$RESET" "$*"; }
warn()    { WARNED=$((WARNED + 1)); printf '  %sWARN%s  %s\n' "$YELLOW" "$RESET" "$*"; }
fail()    { FAILED=$((FAILED + 1)); printf '  %sFAIL%s  %s\n' "$RED" "$RESET" "$*"; }
info()    { printf '        %s%s%s\n' "$DIM" "$*" "$RESET"; }
section() { printf '\n%s%s%s\n' "$BOLD" "$*" "$RESET"; }

# ---- Docker helpers -----------------------------------------------------------------

container_id() { docker compose ps -a -q "$1" 2>/dev/null | head -n 1; }

container_running() {
    local id
    id="$(container_id "$1")"
    [ -n "$id" ] && [ "$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null)" = "running" ]
}

# Run a command in a service container without a TTY
in_container() {
    local service="$1"; shift
    docker compose exec -T "$service" "$@"
}

mysql_query() {
    # MYSQL_PWD avoids the "password on the command line" warning; the variable is set in the container
    in_container mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B -e "$1"' sh "$1"
}

redis_cli() { in_container redis redis-cli "$@"; }

# "used_memory_human:1.2M" -> "1.2M"
info_field() { printf '%s\n' "$1" | tr -d '\r' | awk -F: -v k="$2" '$1 == k { print $2 }'; }

# ---- --trace ------------------------------------------------------------------------

trace_request() {
    section "Log lines for request ID ${TRACE_ID}"
    local found=0 service lines
    lines="$(
        for service in $APP_SERVICES; do
            docker compose logs --no-color --no-log-prefix -t "$service" 2>/dev/null \
                | grep -F "\"requestId\":\"${TRACE_ID}\"" \
                | sed "s/^\([^ ]*\) /\1 ${service} /"
        done | sort
    )"
    if [ -n "$lines" ]; then
        found=1
        # columns: docker timestamp, service, JSON log line
        printf '%s\n' "$lines" | while IFS= read -r line; do
            printf '%s%s%s %s%-22s%s %s\n' "$DIM" "${line%% *}" "$RESET" "$BOLD" "$(echo "$line" | cut -d' ' -f2)" "$RESET" "$(echo "$line" | cut -d' ' -f3-)"
        done
    fi
    if [ "$found" -eq 0 ]; then
        echo "No log lines found for request ID ${TRACE_ID}."
        echo "The ID is returned in the X-Request-Id response header. Logs only cover the containers' current lifetime."
        exit 1
    fi
    exit 0
}

# ---- Checks -------------------------------------------------------------------------

check_docker() {
    section "Docker"
    if ! command -v docker >/dev/null 2>&1; then
        fail "docker CLI not found on PATH"
        return 1
    fi
    if ! docker info >/dev/null 2>&1; then
        fail "Docker daemon not reachable — start Docker Desktop (Windows: try 'wsl --shutdown' first if it is stuck)"
        return 1
    fi
    pass "Docker daemon reachable (server $(docker version --format '{{.Server.Version}}' 2>/dev/null))"

    if ! docker compose version >/dev/null 2>&1; then
        fail "docker compose v2 not available"
        return 1
    fi
    pass "docker compose $(docker compose version --short 2>/dev/null)"

    if [ ! -f .env.docker ]; then
        fail ".env.docker missing — create it with: cp .env.docker.example .env.docker"
        return 1
    fi
    pass ".env.docker present"
}

check_containers() {
    section "Containers"
    local service id state health restarts exit_code oom started
    for service in $ALL_SERVICES; do
        id="$(container_id "$service")"
        if [ -z "$id" ]; then
            fail "$service: no container — run: docker compose up -d --build"
            continue
        fi
        IFS='|' read -r state health restarts exit_code oom started <<EOF
$(docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}|{{.RestartCount}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}' "$id")
EOF
        if [ "$service" = "flights-seeder" ]; then
            if [ "$state" = "exited" ] && [ "$exit_code" = "0" ]; then
                pass "$service: completed"
            elif [ "$state" = "running" ]; then
                warn "$service: still seeding"
            else
                fail "$service: $state (exit $exit_code) — check: docker compose logs flights-seeder"
            fi
            continue
        fi

        if [ "$oom" = "true" ]; then
            fail "$service: killed for running out of memory — give Docker more RAM"
        elif [ "$state" != "running" ]; then
            fail "$service: $state (exit $exit_code) — check: docker compose logs --tail 50 $service"
        elif [ "$health" = "unhealthy" ]; then
            fail "$service: running but unhealthy — check: docker inspect $id --format '{{json .State.Health}}'"
        elif [ "$health" = "starting" ]; then
            warn "$service: health check still starting"
        else
            pass "$service: running$([ "$health" = "healthy" ] && echo ", healthy")"
        fi

        if [ "${restarts:-0}" -gt 0 ]; then
            warn "$service: restarted $restarts time(s) — likely crash loop; last start $started"
        fi
    done
}

http_check() {
    local name="$1" url="$2" result code time
    result="$(curl -s -o /dev/null -w '%{http_code} %{time_total}' --max-time 10 "$url" 2>/dev/null)"
    code="${result%% *}"; time="${result##* }"
    if [ "$code" = "000" ] || [ -z "$code" ]; then
        fail "$name: no response from $url"
    elif [ "$code" -ge 500 ]; then
        warn "$name: HTTP $code from $url (${time}s)"
    else
        pass "$name: HTTP $code (${time}s)"
    fi
}

check_http() {
    section "HTTP endpoints"
    if ! command -v curl >/dev/null 2>&1; then
        warn "curl not installed — skipping HTTP checks"
        return
    fi
    http_check "api-gateway (flight search)" "$GATEWAY_URL/api/v1/flights?travellers=1"
    http_check "flights-service" "$FLIGHTS_URL/api/v1/flights?travellers=1"
    http_check "booking-service" "$BOOKING_URL/api/v1/booking/my-bookings"
    http_check "frontend" "$FRONTEND_URL/"

    # Request IDs are added by the gateway; a missing header means an old image is running
    if curl -s -D - -o /dev/null --max-time 10 "$GATEWAY_URL/api/v1/flights?travellers=1" 2>/dev/null | grep -qi '^x-request-id:'; then
        pass "api-gateway returns X-Request-Id"
    else
        warn "api-gateway response has no X-Request-Id header — rebuild: docker compose up -d --build api-gateway"
    fi
}

check_mysql() {
    section "MySQL"
    if ! container_running mysql; then
        warn "mysql not running — skipped"
        return
    fi
    if ! in_container mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysqladmin -uroot ping' >/dev/null 2>&1; then
        fail "mysqladmin ping failed — server not accepting connections"
        return
    fi
    pass "server accepting connections ($(mysql_query 'SELECT VERSION()' 2>/dev/null | tr -d '\r'))"

    local db dir applied expected
    for pair in auth_db:API-Gateway FLIGHTS:Flights-Service bookings_db:Booking-service; do
        db="${pair%%:*}"; dir="${pair##*:}"
        if [ -z "$(mysql_query "SHOW DATABASES LIKE '$db'" 2>/dev/null | tr -d '\r')" ]; then
            fail "database $db missing — it is created by docker/mysql-init.sql on first start (reset: docker compose down -v)"
            continue
        fi
        applied="$(mysql_query "SELECT COUNT(*) FROM \`$db\`.SequelizeMeta" 2>/dev/null | tr -d '\r')"
        expected="$(find "$dir/src/migrations" -name '*.js' 2>/dev/null | wc -l | tr -d ' ')"
        if [ -z "$applied" ]; then
            fail "$db: no migrations table — the service's migrations never ran"
        elif [ "$applied" -lt "$expected" ]; then
            fail "$db: $applied of $expected migrations applied — restart $dir's container to run them"
        else
            pass "$db: migrations up to date ($applied/$expected)"
        fi
    done

    local upcoming
    upcoming="$(mysql_query 'SELECT COUNT(*) FROM FLIGHTS.Flights WHERE departureTime > NOW()' 2>/dev/null | tr -d '\r')"
    if [ -z "$upcoming" ]; then
        warn "could not count flights"
    elif [ "$upcoming" -eq 0 ]; then
        warn "no upcoming flights — seed data has aged out; reseed with: docker compose down -v && docker compose up -d"
    else
        pass "$upcoming upcoming flights"
    fi

    local breakdown stale
    breakdown="$(mysql_query 'SELECT status, COUNT(*) FROM bookings_db.Bookings GROUP BY status' 2>/dev/null | tr -d '\r' | awk '{ printf "%s%s=%s", sep, $1, $2; sep=", " }')"
    info "bookings: ${breakdown:-none}"
    stale="$(mysql_query "SELECT COUNT(*) FROM bookings_db.Bookings WHERE status IN ('initiated','pending') AND createdAt < NOW() - INTERVAL $STALE_BOOKING_MINUTES MINUTE" 2>/dev/null | tr -d '\r')"
    if [ -n "$stale" ] && [ "$stale" -gt 0 ]; then
        warn "$stale unpaid booking(s) older than ${STALE_BOOKING_MINUTES}m still hold seats — the cleanup cron in booking-service isn't cancelling them"
    else
        pass "no stale unpaid bookings"
    fi

    local threads max
    threads="$(mysql_query "SHOW STATUS LIKE 'Threads_connected'" 2>/dev/null | tr -d '\r' | awk '{ print $2 }')"
    max="$(mysql_query 'SELECT @@max_connections' 2>/dev/null | tr -d '\r')"
    if [ -n "$threads" ] && [ -n "$max" ]; then
        if [ $((threads * 100 / max)) -ge 80 ]; then
            warn "connections: $threads of $max in use"
        else
            pass "connections: $threads of $max in use"
        fi
    fi
}

check_redis() {
    section "Redis"
    if ! container_running redis; then
        warn "redis not running — skipped"
        return
    fi
    if [ "$(redis_cli PING 2>/dev/null | tr -d '\r')" != "PONG" ]; then
        fail "PING failed"
        return
    fi
    pass "PING ok"

    local mem stats persistence clients
    mem="$(redis_cli INFO memory 2>/dev/null)"
    stats="$(redis_cli INFO stats 2>/dev/null)"
    persistence="$(redis_cli INFO persistence 2>/dev/null)"
    clients="$(redis_cli INFO clients 2>/dev/null)"

    info "memory: $(info_field "$mem" used_memory_human) used, maxmemory $(info_field "$mem" maxmemory_human), policy $(info_field "$mem" maxmemory_policy)"
    info "clients: $(info_field "$clients" connected_clients) connected"

    local evicted rejected bgsave
    evicted="$(info_field "$stats" evicted_keys)"
    rejected="$(info_field "$stats" rejected_connections)"
    bgsave="$(info_field "$persistence" rdb_last_bgsave_status)"

    if [ "${evicted:-0}" -gt 0 ]; then
        warn "$evicted keys evicted — idempotency records may have been dropped early"
    else
        pass "no evicted keys"
    fi
    if [ "${rejected:-0}" -gt 0 ]; then
        warn "$rejected connections rejected (maxclients reached)"
    fi
    if [ -n "$bgsave" ] && [ "$bgsave" != "ok" ]; then
        fail "last background save: $bgsave — Redis may refuse writes (MISCONF); check disk space"
    else
        pass "persistence ok"
    fi

    local pattern label count
    for pair in 'idempotency:*|idempotency records' 'blacklist:*|revoked tokens' 'flights:search:*|cached flight searches' 'rl:*|rate-limit counters'; do
        pattern="${pair%%|*}"; label="${pair##*|}"
        count="$(redis_cli --scan --pattern "$pattern" 2>/dev/null | wc -l | tr -d ' ')"
        info "$label: $count"
    done
}

check_rabbitmq() {
    section "RabbitMQ"
    if ! container_running rabbitmq; then
        warn "rabbitmq not running — skipped"
        return
    fi
    if ! in_container rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        fail "node not responding to ping"
        return
    fi
    pass "node responding"

    if in_container rabbitmq rabbitmq-diagnostics -q check_local_alarms >/dev/null 2>&1; then
        pass "no memory/disk alarms"
    else
        fail "memory or disk alarm active — publishers are blocked; see: docker compose exec rabbitmq rabbitmq-diagnostics alarms"
    fi

    local queues
    queues="$(in_container rabbitmq rabbitmqctl -q list_queues --no-table-headers name durable messages_ready messages_unacknowledged consumers 2>/dev/null | tr -d '\r')"

    local queue consumer row durable ready unacked consumers
    for pair in seat.restoration:flights-service booking.confirmed:notifications-service; do
        queue="${pair%%:*}"; consumer="${pair##*:}"
        row="$(printf '%s\n' "$queues" | awk -v q="$queue" '$1 == q')"
        if [ -z "$row" ]; then
            fail "$queue: queue missing — it is created when booking-service or $consumer connects"
            continue
        fi
        read -r _ durable ready unacked consumers <<EOF
$row
EOF
        if [ "$consumers" -eq 0 ]; then
            fail "$queue: no consumers — $consumer is down or lost its subscription; $ready message(s) waiting"
        elif [ "$ready" -ge "$QUEUE_BACKLOG_WARN" ]; then
            warn "$queue: backlog of $ready ready message(s), $unacked in flight, $consumers consumer(s)"
        else
            pass "$queue: $consumers consumer(s), $ready ready, $unacked in flight"
        fi
        if [ "$durable" != "true" ]; then
            warn "$queue: not durable — messages are lost if RabbitMQ restarts"
        fi
    done

    local connections
    connections="$(in_container rabbitmq rabbitmqctl -q list_connections --no-table-headers name 2>/dev/null | tr -d '\r' | grep -c .)"
    if [ "$connections" -lt 3 ]; then
        warn "$connections client connection(s); expected 3 (booking, flights, notifications)"
    else
        pass "$connections client connections"
    fi
}

check_logs() {
    section "Errors in the last $SINCE"
    local service logs errors
    for service in $APP_SERVICES; do
        if [ -z "$(container_id "$service")" ]; then
            continue
        fi
        logs="$(docker compose logs --no-color --no-log-prefix --since "$SINCE" "$service" 2>/dev/null)"
        errors="$(printf '%s\n' "$logs" | grep -c '"level":"error"')"
        if [ "$errors" -eq 0 ]; then
            pass "$service: no errors"
        else
            warn "$service: $errors error line(s)"
            # Show the most recent few as: message (requestId)
            printf '%s\n' "$logs" | grep '"level":"error"' | tail -n 3 | while IFS= read -r line; do
                info "$(printf '%s' "$line" | sed -n 's/.*"message":"\([^"]*\)".*/\1/p') $(printf '%s' "$line" | sed -n 's/.*"requestId":"\([^"]*\)".*/(requestId \1)/p')"
            done
        fi
    done
    info "trace one request with: scripts/diagnose.sh --trace <requestId>"
}

# ---- Main ---------------------------------------------------------------------------

if ! check_docker; then
    printf '\n%sCannot continue without Docker.%s\n' "$RED" "$RESET"
    exit 1
fi

[ -n "$TRACE_ID" ] && trace_request

check_containers
check_http
check_mysql
check_redis
check_rabbitmq
check_logs

printf '\n%sSummary:%s %s passed, %s warning(s), %s failed\n' "$BOLD" "$RESET" "$PASSED" "$WARNED" "$FAILED"
[ "$FAILED" -eq 0 ]
