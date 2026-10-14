/*
 * Helpers for the integration tests, which drive the real Docker Compose stack:
 * HTTP calls through the API Gateway, RabbitMQ's management API, and docker compose
 * to stop/start/restart containers.
 */

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { ROOT, waitFor } = require('./load-with-mocks');

const GATEWAY_URL = process.env.GATEWAY_URL || 'http://localhost:5000';
const FLIGHTS_URL = process.env.FLIGHTS_URL || 'http://localhost:3000';
const RABBITMQ_API_URL = process.env.RABBITMQ_API_URL || 'http://localhost:15672';
const RABBITMQ_API_AUTH = 'Basic ' + Buffer.from(process.env.RABBITMQ_API_CREDENTIALS || 'guest:guest').toString('base64');

function compose(...args) {
    return execFileSync('docker', ['compose', ...args], {
        cwd: ROOT,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
    });
}

async function request(baseUrl, method, path, { token, body, headers = {} } = {}) {
    const res = await fetch(baseUrl + path, {
        method,
        headers: {
            ...(body ? { 'Content-Type': 'application/json' } : {}),
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
            ...headers,
        },
        body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = text; }
    return { status: res.status, headers: res.headers, body: parsed };
}

const gateway = (method, path, options) => request(GATEWAY_URL, method, path, options);

async function rabbitApi(method, path, body) {
    const res = await fetch(`${RABBITMQ_API_URL}/api${path}`, {
        method,
        headers: { Authorization: RABBITMQ_API_AUTH, 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) throw new Error(`RabbitMQ API ${method} ${path} -> ${res.status}`);
    return res.json();
}

async function assertStackIsUp() {
    try {
        compose('version');
    } catch {
        throw new Error('docker compose is not available. Install Docker and run from the repo root.');
    }
    try {
        await fetch(`${GATEWAY_URL}/api/v1/flights?travellers=1`);
        await rabbitApi('GET', '/overview');
    } catch (err) {
        throw new Error(`Stack not reachable (${err.message}). Start it first: docker compose up -d --build`);
    }
}

// ---- Users, flights and bookings -------------------------------------------------

async function registerUser() {
    const email = `recovery-${randomUUID()}@example.com`;
    const res = await gateway('POST', '/api/v1/auth/register', { body: { email, password: 'recovery-test-pw' } });
    assert.equal(res.status, 201, `register failed: ${JSON.stringify(res.body)}`);
    return { token: res.body.token, userId: res.body.userId, email };
}

async function getSeats(flightId, seatClass) {
    const res = await request(FLIGHTS_URL, 'GET', `/api/v1/flights/${flightId}`);
    assert.equal(res.status, 200, `get flight failed: ${JSON.stringify(res.body)}`);
    return res.body.data.FlightClasses.find((c) => c.seatClass === seatClass).totalSeats;
}

// Goes straight to the Flights Service so polling doesn't eat the gateway's rate limit
async function findBookableFlight(seatClass = 'economy', minSeats = 10) {
    const res = await request(FLIGHTS_URL, 'GET', `/api/v1/flights?travellers=${minSeats}&seatClass=${seatClass}`);
    assert.equal(res.status, 200, `flight search failed: ${JSON.stringify(res.body)}`);
    // Search results are cached, so confirm the live seat count before using a flight
    for (const flight of res.body.data.slice(0, 20)) {
        if (await getSeats(flight.id, seatClass) >= minSeats) return { flightId: flight.id, seatClass };
    }
    throw new Error(`No upcoming flight with ${minSeats}+ ${seatClass} seats. Re-seed: docker compose down -v && docker compose up -d`);
}

async function createBooking(user, flight) {
    const res = await gateway('POST', '/api/v1/booking', {
        token: user.token,
        body: { flightId: flight.flightId, noofSeats: 1, seatClass: flight.seatClass },
    });
    assert.equal(res.status, 200, `create booking failed: ${JSON.stringify(res.body)}`);
    return res.body.data;
}

function pay(user, booking, { key = randomUUID(), totalCost = booking.totalCost, requestId } = {}) {
    return gateway('POST', '/api/v1/booking/payment', {
        token: user.token,
        body: { bookingId: booking.id, totalCost },
        headers: { 'Idempotency-Key': key, ...(requestId ? { 'X-Request-Id': requestId } : {}) },
    });
}

async function bookingStatus(user, bookingId) {
    const res = await gateway('GET', '/api/v1/booking/my-bookings', { token: user.token });
    assert.equal(res.status, 200);
    return res.body.data.find((b) => b.id === bookingId)?.status;
}

// ---- RabbitMQ ---------------------------------------------------------------------

// Note: the management API refreshes these counters every few seconds, so poll them
async function queueStats(queue) {
    const q = await rabbitApi('GET', `/queues/%2F/${encodeURIComponent(queue)}`);
    return { ready: q.messages_ready ?? 0, unacked: q.messages_unacknowledged ?? 0, consumers: q.consumers ?? 0 };
}

/*
 * Reads the messages waiting in a queue without consuming them (they are requeued).
 * Only use it while the queue's consumer is stopped, otherwise the consumer may race us.
 */
async function peekMessages(queue) {
    const messages = await rabbitApi('POST', `/queues/%2F/${encodeURIComponent(queue)}/get`, {
        count: 1000,
        ackmode: 'ack_requeue_true',
        encoding: 'auto',
        truncate: 50000,
    });
    return messages.map((m) => ({ payload: JSON.parse(m.payload), headers: m.properties?.headers || {} }));
}

async function messagesForBooking(queue, bookingId) {
    return (await peekMessages(queue)).filter((m) => m.payload.bookingId === bookingId);
}

// ---- Containers -------------------------------------------------------------------

function stopService(service) {
    compose('stop', service);
}

function startService(service) {
    compose('start', service);
}

function restartCount(service) {
    const containerId = compose('ps', '-q', service).trim();
    return Number(execFileSync('docker', ['inspect', '-f', '{{.RestartCount}}', containerId], { encoding: 'utf8' }).trim());
}

async function restartRabbitMQ() {
    compose('restart', 'rabbitmq');
    await waitFor(async () => {
        try { await rabbitApi('GET', '/overview'); return true; } catch { return false; }
    }, { timeoutMs: 120000, intervalMs: 1000, description: 'RabbitMQ to come back' });
}

// Structured (JSON) log lines a service has written since `sinceIso`
function serviceLogs(service, sinceIso) {
    const output = compose('logs', '--no-color', '--no-log-prefix', '--since', sinceIso, service);
    return output.split('\n').flatMap((line) => {
        try { return [JSON.parse(line)]; } catch { return []; }
    });
}

async function waitForLog(service, sinceIso, predicate, description, timeoutMs = 60000) {
    let match;
    await waitFor(() => {
        match = serviceLogs(service, sinceIso).find(predicate);
        return Boolean(match);
    }, { timeoutMs, intervalMs: 1000, description });
    return match;
}

module.exports = {
    compose,
    gateway,
    assertStackIsUp,
    registerUser,
    getSeats,
    findBookableFlight,
    createBooking,
    pay,
    bookingStatus,
    queueStats,
    peekMessages,
    messagesForBooking,
    stopService,
    startService,
    restartCount,
    restartRabbitMQ,
    serviceLogs,
    waitForLog,
    waitFor,
};
