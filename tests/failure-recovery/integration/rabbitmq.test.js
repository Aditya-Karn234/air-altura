/*
 * RabbitMQ failure recovery against the real stack: consumers going down, the broker
 * restarting, and services reconnecting afterwards.
 *
 * These tests stop and restart containers. Run them against a local stack only.
 *
 * Requires: docker compose up -d --build
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const stack = require('../helpers/stack');

const TIMEOUT = { timeout: 300000 };

describe('RabbitMQ failure recovery (integration)', () => {
    let user;
    let flight;
    let stackWasUp = false;

    before(async () => {
        await stack.assertStackIsUp();
        stackWasUp = true;
        user = await stack.registerUser();
        flight = await stack.findBookableFlight();
    });

    // Leave the stack running however the tests end
    after(() => {
        if (stackWasUp) stack.compose('start', 'rabbitmq', 'flights-service', 'booking-service', 'notifications-service');
    });

    it('holds seat restorations while the Flights Service is down and applies them when it returns', TIMEOUT, async () => {
        const seatsBefore = await stack.getSeats(flight.flightId, flight.seatClass);
        const booking = await stack.createBooking(user, flight);
        assert.equal(await stack.getSeats(flight.flightId, flight.seatClass), seatsBefore - 1);

        stack.stopService('flights-service');
        try {
            const cancel = await stack.gateway('POST', '/api/v1/booking/cancel', {
                token: user.token,
                body: { bookingId: booking.id },
            });
            assert.equal(cancel.status, 200, JSON.stringify(cancel.body));

            const waiting = await stack.messagesForBooking('seat.restoration', booking.id);
            assert.equal(waiting.length, 1, 'restoration event should be waiting in the queue');
        } finally {
            stack.startService('flights-service');
        }

        await stack.waitFor(async () => {
            try { return await stack.getSeats(flight.flightId, flight.seatClass) === seatsBefore; } catch { return false; }
        }, { timeoutMs: 120000, intervalMs: 2000, description: 'seats to be restored after Flights Service restart' });
    });

    it('keeps persistent messages across a RabbitMQ restart', TIMEOUT, async () => {
        stack.stopService('notifications-service');
        try {
            const booking = await stack.createBooking(user, flight);
            assert.equal((await stack.pay(user, booking)).status, 200);
            assert.equal((await stack.messagesForBooking('booking.confirmed', booking.id)).length, 1);

            await stack.restartRabbitMQ();

            assert.equal((await stack.messagesForBooking('booking.confirmed', booking.id)).length, 1,
                'message was lost in the broker restart');
        } finally {
            stack.startService('notifications-service');
        }
    });

    it('reconnects publishers and consumers after a RabbitMQ restart, without crashing', TIMEOUT, async () => {
        const services = ['booking-service', 'flights-service', 'notifications-service'];
        const restartsBefore = Object.fromEntries(services.map((s) => [s, stack.restartCount(s)]));
        const since = new Date().toISOString();

        await stack.restartRabbitMQ();

        // Each service logs this after reconnecting (default delay: 5s)
        for (const service of services) {
            await stack.waitForLog(service, since, (l) => l.message === 'RabbitMQ connected',
                `${service} to reconnect to RabbitMQ`, 90000);
        }
        await stack.waitFor(async () => {
            const [restoration, confirmed] = await Promise.all([
                stack.queueStats('seat.restoration'),
                stack.queueStats('booking.confirmed'),
            ]);
            return restoration.consumers >= 1 && confirmed.consumers >= 1;
        }, { timeoutMs: 60000, intervalMs: 2000, description: 'consumers to re-attach to both queues' });

        // End to end after the restart: pay (booking.confirmed), then cancel (seat.restoration)
        const requestId = `recovery-test-${randomUUID()}`;
        const seatsBefore = await stack.getSeats(flight.flightId, flight.seatClass);
        const booking = await stack.createBooking(user, flight);
        assert.equal((await stack.pay(user, booking, { requestId })).status, 200);

        await stack.waitForLog('notifications-service', since, (l) => l.requestId === requestId,
            'notifications service to process the post-restart confirmation');

        const cancel = await stack.gateway('POST', '/api/v1/booking/cancel', { token: user.token, body: { bookingId: booking.id } });
        assert.equal(cancel.status, 200);
        await stack.waitFor(async () => await stack.getSeats(flight.flightId, flight.seatClass) === seatsBefore, {
            timeoutMs: 60000, intervalMs: 2000, description: 'flights service to process the post-restart seat restoration',
        });

        for (const service of services) {
            assert.equal(stack.restartCount(service), restartsBefore[service], `${service} crashed and was restarted by Docker`);
        }
    });
});
