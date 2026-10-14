/*
 * Idempotency Middleware
 *
 * Protects state-changing endpoints (e.g. makePayment) from being executed
 * more than once for the same logical request — even if the client retries
 * due to a network timeout, double-click, or frontend bug.
 *
 * How it works:
 *  1. Client generates a UUID before making the request and sends it as:
 *       Idempotency-Key: <uuid>
 *  2. Middleware atomically claims the key in Redis (SET NX) with an IN_PROGRESS marker.
 *     - Claim WON          → let request through, intercept the response:
 *                              2xx  → cache { statusCode, body } for 24hrs
 *                              else → release the key so the client can retry
 *     - Key IN_PROGRESS    → another request with this key is still running → 409
 *     - Key has a result   → return the cached response immediately, skip handler
 *
 * Why an atomic claim instead of GET-then-SET:
 *  - Two concurrent retries (double-click) would both see "no key" with a plain GET,
 *    both run the payment, and both publish booking.confirmed. SET NX lets exactly one win.
 *
 * Why the key is scoped by user:
 *  - Without it, a user who reused another user's key would be served that user's response.
 *
 * Why 24 hours TTL:
 *  - Retries for a payment almost never happen after 24 hours.
 *  - Keeps Redis memory bounded — keys expire automatically.
 *
 * Why the in-progress marker has a short TTL:
 *  - If the process dies mid-request, the key frees itself instead of blocking retries for 24hrs.
 *
 * Why we intercept res.json:
 *  - Express doesn't expose a built-in "after response" hook.
 *  - We wrap res.json so we can capture the status code and body
 *    at the exact moment the handler sends its response, then store it.
 */

const { RedisClient, Logger } = require('../config');

const IN_PROGRESS = '__in_progress__';
const IN_PROGRESS_TTL_SECONDS = 60;
const RESULT_TTL_SECONDS = 86400;

async function idempotencyMiddleware(req, res, next) {
    const key = req.headers['idempotency-key'];

    // If no key provided, reject the request — the client must supply one
    if (!key) {
        return res.status(400).json({
            message: 'Idempotency-Key header is required for this endpoint'
        });
    }

    const redisKey = `idempotency:${req.headers['x-user-id'] || 'anonymous'}:${key}`;

    let claimed;
    try {
        claimed = await RedisClient.set(redisKey, IN_PROGRESS, 'EX', IN_PROGRESS_TTL_SECONDS, 'NX');

        if (!claimed) {
            const cached = await RedisClient.get(redisKey);

            // cached is null only if the marker expired between SET and GET; treat it as in progress
            if (cached === IN_PROGRESS || cached === null) {
                Logger.warn('Idempotent request already in progress', { idempotencyKey: key });
                res.set('Retry-After', '1');
                return res.status(409).json({
                    message: 'A request with this Idempotency-Key is already being processed'
                });
            }

            // We've seen this key before — return the original response
            const { statusCode, body } = JSON.parse(cached);
            Logger.info('Replaying cached idempotent response', { idempotencyKey: key, statusCode });
            res.set('X-Request-Cached', 'true');
            return res.status(statusCode).json(body);
        }
    } catch (error) {
        // If Redis is down, fail open — let the request through
        // A Redis outage should not bring down the payment endpoint
        Logger.error('Idempotency middleware Redis error, failing open', { idempotencyKey: key, err: error });
        return next();
    }

    // Claim won — intercept res.json to capture the response
    const originalJson = res.json.bind(res);
    res.json = async (body) => {
        // Snapshot now: controllers reuse shared response objects, which another
        // request could mutate while we await Redis below.
        const snapshot = JSON.parse(JSON.stringify(body));
        try {
            // Only cache successful responses (2xx)
            // Failed requests should NOT be cached — client must be able to retry
            // with the same key after fixing the issue (e.g. wrong totalCost)
            if (res.statusCode >= 200 && res.statusCode < 300) {
                await RedisClient.set(
                    redisKey,
                    JSON.stringify({ statusCode: res.statusCode, body: snapshot }),
                    'EX',
                    RESULT_TTL_SECONDS
                );
            } else {
                await RedisClient.del(redisKey);
            }
        } catch (error) {
            Logger.error('Failed to store idempotent response', { idempotencyKey: key, err: error });
        }
        return originalJson(snapshot);
    };

    next();
}

module.exports = idempotencyMiddleware;
