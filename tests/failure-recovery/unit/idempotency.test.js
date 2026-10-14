/*
 * Booking idempotency — failure and retry behaviour of the payment endpoint's middleware.
 *
 * Runs the real middleware inside a real Express server, with Redis replaced by an
 * in-memory fake that can be told to fail.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const { loadWithMocks, servicePath, createRecordingLogger } = require('../helpers/load-with-mocks');

const express = require(require.resolve('express', { paths: [servicePath('Booking-service')] }));

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class FakeRedis {
    constructor() {
        this.store = new Map();
        this.ttls = new Map();
        this.down = false;            // every command fails, like a lost connection
        this.failResultWrites = false; // only the post-response write fails
    }

    async tick() {
        await new Promise((resolve) => setImmediate(resolve));
        if (this.down) throw new Error('connect ECONNREFUSED 127.0.0.1:6379');
    }

    async set(key, value, ...args) {
        await this.tick();
        const nx = args.includes('NX');
        if (!nx && this.failResultWrites) throw new Error('READONLY You can\'t write against a read only replica');
        if (nx && this.store.has(key)) return null;
        this.store.set(key, value);
        const ex = args.indexOf('EX');
        if (ex !== -1) this.ttls.set(key, args[ex + 1]);
        return 'OK';
    }

    async get(key) {
        await this.tick();
        return this.store.has(key) ? this.store.get(key) : null;
    }

    async del(key) {
        await this.tick();
        this.ttls.delete(key);
        return this.store.delete(key) ? 1 : 0;
    }
}

// Stand-in for the payment handler: fails on a wrong amount, otherwise "charges" once per call
function startServer(redis) {
    const logger = createRecordingLogger();
    const idempotency = loadWithMocks('Booking-service', 'src/middlewares/idempotency.js', [
        [servicePath('Booking-service/src/config/index.js'), { RedisClient: redis, Logger: logger }],
    ]);

    const charges = [];
    const app = express();
    app.use(express.json());
    app.post('/payment', idempotency, async (req, res) => {
        await delay(req.body.delayMs || 0);
        if (req.body.totalCost !== 100) {
            return res.status(400).json({ success: false, message: 'The amount of the payment doesnt match' });
        }
        charges.push({ bookingId: req.body.bookingId, userId: req.headers['x-user-id'] });
        return res.status(200).json({ success: true, data: { bookingId: req.body.bookingId, chargeNo: charges.length } });
    });

    return new Promise((resolve) => {
        const server = app.listen(0, '127.0.0.1', () => {
            const url = `http://127.0.0.1:${server.address().port}`;
            resolve({ url, charges, logger, close: () => new Promise((r) => server.close(r)) });
        });
    });
}

async function pay(url, { key, userId = '1', body = {} } = {}) {
    const headers = { 'Content-Type': 'application/json', 'x-user-id': userId };
    if (key) headers['Idempotency-Key'] = key;
    const res = await fetch(`${url}/payment`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ bookingId: 42, totalCost: 100, ...body }),
    });
    return { status: res.status, cached: res.headers.get('x-request-cached') === 'true', body: await res.json() };
}

describe('booking idempotency middleware', () => {
    let redis;
    let server;

    beforeEach(async () => {
        redis = new FakeRedis();
        server = await startServer(redis);
    });

    afterEach(async () => {
        await server.close();
    });

    it('rejects a payment without an Idempotency-Key', async () => {
        const res = await pay(server.url);
        assert.equal(res.status, 400);
        assert.equal(server.charges.length, 0);
    });

    it('replays the original response when a client retries after a timeout', async () => {
        const first = await pay(server.url, { key: 'retry-1' });
        const retry = await pay(server.url, { key: 'retry-1' });

        assert.equal(first.status, 200);
        assert.equal(retry.status, 200);
        assert.equal(retry.cached, true);
        assert.deepEqual(retry.body, first.body);
        assert.equal(server.charges.length, 1, 'payment must run exactly once');
    });

    it('lets only one of several concurrent duplicates through (double-click)', async () => {
        const results = await Promise.all(
            Array.from({ length: 5 }, () => pay(server.url, { key: 'double-click', body: { delayMs: 100 } }))
        );

        assert.equal(server.charges.length, 1, 'payment must run exactly once');
        assert.equal(results.filter((r) => r.status === 200).length, 1);
        assert.equal(results.filter((r) => r.status === 409).length, 4);

        // Once the first finishes, a retry gets the stored result instead of a 409
        const later = await pay(server.url, { key: 'double-click' });
        assert.equal(later.status, 200);
        assert.equal(later.cached, true);
    });

    it('does not cache a failed payment, so the same key can be retried after fixing it', async () => {
        const failed = await pay(server.url, { key: 'fix-and-retry', body: { totalCost: 99 } });
        assert.equal(failed.status, 400);

        const fixed = await pay(server.url, { key: 'fix-and-retry' });
        assert.equal(fixed.status, 200);
        assert.equal(fixed.cached, false);
        assert.equal(server.charges.length, 1);
    });

    it('does not serve one user\'s cached response to another user reusing the key', async () => {
        await pay(server.url, { key: 'shared-key', userId: '1' });
        const other = await pay(server.url, { key: 'shared-key', userId: '2' });

        assert.equal(other.cached, false);
        assert.deepEqual(server.charges.map((c) => c.userId), ['1', '2']);
    });

    it('marks in-flight keys with a short TTL so a crashed request cannot block retries for a day', async () => {
        await pay(server.url, { key: 'ttl-check', body: { delayMs: 50 } }).then(() => {});
        // The final result is stored for 24h...
        assert.equal(redis.ttls.get('idempotency:1:ttl-check'), 86400);

        // ...but the in-progress marker is short-lived. Simulate a process that died mid-request.
        const pending = pay(server.url, { key: 'crashed', body: { delayMs: 50 } });
        await delay(10);
        assert.equal(redis.store.get('idempotency:1:crashed'), '__in_progress__');
        assert.ok(redis.ttls.get('idempotency:1:crashed') <= 60);
        await pending;
    });

    it('fails open when Redis is down, so payments keep working', async () => {
        redis.down = true;
        const res = await pay(server.url, { key: 'redis-down' });

        assert.equal(res.status, 200);
        assert.equal(server.charges.length, 1);
        assert.ok(server.logger.records.some((r) => r.level === 'error' && /failing open/.test(r.message)));
    });

    it('recovers once Redis comes back: new keys are protected again', async () => {
        redis.down = true;
        await pay(server.url, { key: 'during-outage' });
        redis.down = false;

        await pay(server.url, { key: 'after-outage' });
        const retry = await pay(server.url, { key: 'after-outage' });
        assert.equal(retry.cached, true);
        assert.equal(server.charges.length, 2);
    });

    it('still returns the payment response if storing it in Redis fails', async () => {
        redis.failResultWrites = true;
        const res = await pay(server.url, { key: 'store-fails' });

        assert.equal(res.status, 200);
        assert.equal(res.body.success, true);
        assert.ok(server.logger.records.some((r) => /Failed to store idempotent response/.test(r.message)));
    });
});
