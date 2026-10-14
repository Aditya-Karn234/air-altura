/*
 * Booking idempotency against the real stack (gateway → booking service → Redis/MySQL/RabbitMQ).
 *
 * The Notifications Service is stopped for the duration, so every booking.confirmed event
 * stays in the queue where we can count it. It is started again at the end, and the last
 * test checks that the backlog is delivered.
 *
 * Requires: docker compose up -d --build
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const stack = require('../helpers/stack');

const TIMEOUT = { timeout: 180000 };

describe('booking idempotency (integration)', () => {
    let user;
    let flight;
    let notificationsStopped = false;
    const requestIds = [];

    before(async () => {
        await stack.assertStackIsUp();
        user = await stack.registerUser();
        flight = await stack.findBookableFlight();
        stack.stopService('notifications-service');
        notificationsStopped = true;
    });

    after(() => {
        if (notificationsStopped) stack.startService('notifications-service');
    });

    it('charges a retried payment once and publishes one confirmation', TIMEOUT, async () => {
        const booking = await stack.createBooking(user, flight);
        const key = randomUUID();

        const first = await stack.pay(user, booking, { key });
        const retry = await stack.pay(user, booking, { key });

        assert.equal(first.status, 200, JSON.stringify(first.body));
        assert.equal(retry.status, 200);
        assert.equal(retry.headers.get('x-request-cached'), 'true');
        assert.deepEqual(retry.body, first.body);
        assert.equal(await stack.bookingStatus(user, booking.id), 'booked');
        assert.equal((await stack.messagesForBooking('booking.confirmed', booking.id)).length, 1);
    });

    it('lets one of five concurrent duplicate payments through (double-click)', TIMEOUT, async () => {
        const booking = await stack.createBooking(user, flight);
        const key = randomUUID();

        const results = await Promise.all(Array.from({ length: 5 }, () => stack.pay(user, booking, { key })));

        const statuses = results.map((r) => r.status);
        assert.ok(statuses.every((s) => s === 200 || s === 409), `unexpected statuses ${statuses}`);
        const processed = results.filter((r) => r.status === 200 && r.headers.get('x-request-cached') !== 'true');
        assert.equal(processed.length, 1, `exactly one request should run the payment, got ${statuses}`);
        assert.equal((await stack.messagesForBooking('booking.confirmed', booking.id)).length, 1);
    });

    it('lets a rejected payment be retried with the same key', TIMEOUT, async () => {
        const booking = await stack.createBooking(user, flight);
        const key = randomUUID();

        const wrongAmount = await stack.pay(user, booking, { key, totalCost: booking.totalCost + 1 });
        assert.equal(wrongAmount.status, 400);
        assert.equal(await stack.bookingStatus(user, booking.id), 'initiated');

        const fixed = await stack.pay(user, booking, { key });
        assert.equal(fixed.status, 200, JSON.stringify(fixed.body));
        assert.notEqual(fixed.headers.get('x-request-cached'), 'true');
        assert.equal(await stack.bookingStatus(user, booking.id), 'booked');
    });

    it('does not let a key used by one user short-circuit another user\'s payment', TIMEOUT, async () => {
        const otherUser = await stack.registerUser();
        const mine = await stack.createBooking(user, flight);
        const theirs = await stack.createBooking(otherUser, flight);
        const key = randomUUID();

        assert.equal((await stack.pay(user, mine, { key })).status, 200);
        const res = await stack.pay(otherUser, theirs, { key });

        assert.equal(res.status, 200, JSON.stringify(res.body));
        assert.notEqual(res.headers.get('x-request-cached'), 'true', 'served another user\'s cached response');
        assert.equal(await stack.bookingStatus(otherUser, theirs.id), 'booked');
    });

    it('stamps the confirmation event with the payment\'s request ID', TIMEOUT, async () => {
        const booking = await stack.createBooking(user, flight);
        const requestId = `recovery-test-${randomUUID()}`;
        requestIds.push(requestId);

        const res = await stack.pay(user, booking, { requestId });
        assert.equal(res.status, 200);
        assert.equal(res.headers.get('x-request-id'), requestId);

        const [message] = await stack.messagesForBooking('booking.confirmed', booking.id);
        assert.equal(message.headers['x-request-id'], requestId);
    });

    it('delivers the queued confirmations once the Notifications Service is back', TIMEOUT, async () => {
        const since = new Date().toISOString();
        stack.startService('notifications-service');

        await stack.waitFor(async () => (await stack.queueStats('booking.confirmed')).ready === 0, {
            timeoutMs: 90000, intervalMs: 2000, description: 'booking.confirmed backlog to drain',
        });

        // Whether the email send succeeds depends on RESEND_API_KEY; either way it is logged
        // under the payment's request ID, which proves the ID crossed the queue.
        const [requestId] = requestIds;
        await stack.waitForLog('notifications-service', since, (l) => l.requestId === requestId,
            `notifications log line with requestId ${requestId}`);
    });
});
