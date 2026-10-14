/*
 * RabbitMQ message processing — how each consumer acks, retries or discards messages
 * when its downstream dependency fails, and whether the request ID survives the hop.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { loadWithMocks, servicePath, createRecordingLogger } = require('../helpers/load-with-mocks');

function fakeChannel() {
    const acks = [];
    const nacks = [];
    return {
        acks,
        nacks,
        ack: (msg) => acks.push(msg),
        nack: (msg, allUpTo, requeue) => nacks.push({ msg, requeue }),
    };
}

function message(payload, { requestId, redelivered = false } = {}) {
    const content = Buffer.from(typeof payload === 'string' ? payload : JSON.stringify(payload));
    return {
        content,
        fields: { redelivered },
        properties: { headers: requestId ? { 'x-request-id': requestId } : {} },
    };
}

describe('seat.restoration consumer (Flights Service)', () => {
    function load(updateSeats) {
        const { getRequestId } = require(servicePath('Flights-Service/src/utils/request-context.js'));
        const logger = createRecordingLogger(getRequestId);
        const { handleSeatRestoration } = loadWithMocks('Flights-Service', 'src/subscribers/seat-restoration.js', [
            [servicePath('Flights-Service/src/config/index.js'), { RabbitMQ: {}, Logger: logger }],
            [servicePath('Flights-Service/src/services/index.js'), { FlightService: { updateSeats } }],
            [servicePath('Flights-Service/src/utils/request-context.js'), require(servicePath('Flights-Service/src/utils/request-context.js'))],
        ]);
        return { handleSeatRestoration, logger };
    }

    const event = { bookingId: 7, flightId: 3, seats: 2, seatClass: 'economy' };

    it('restores seats for the right cabin and acks', async () => {
        const calls = [];
        const { handleSeatRestoration } = load(async (data) => calls.push(data));
        const channel = fakeChannel();

        await handleSeatRestoration(channel, message(event));

        assert.deepEqual(calls, [{ flightId: 3, seats: 2, dec: false, seatClass: 'economy' }]);
        assert.equal(channel.acks.length, 1);
        assert.equal(channel.nacks.length, 0);
    });

    it('requeues the message when the database is down, and succeeds on redelivery', async () => {
        let dbUp = false;
        const restored = [];
        const { handleSeatRestoration } = load(async (data) => {
            if (!dbUp) throw new Error('ConnectionRefusedError: connect ECONNREFUSED mysql:3306');
            restored.push(data);
        });
        const channel = fakeChannel();

        await handleSeatRestoration(channel, message(event));
        assert.deepEqual(channel.nacks.map((n) => n.requeue), [true], 'must requeue, or the seats are lost for good');
        assert.equal(channel.acks.length, 0);

        dbUp = true;
        await handleSeatRestoration(channel, message(event, { redelivered: true }));
        assert.equal(channel.acks.length, 1);
        assert.equal(restored.length, 1);
    });

    it('logs under the request ID carried in the message headers', async () => {
        const { handleSeatRestoration, logger } = load(async () => {});

        await handleSeatRestoration(fakeChannel(), message(event, { requestId: 'req-from-booking' }));

        assert.ok(logger.records.length > 0);
        for (const record of logger.records) assert.equal(record.requestId, 'req-from-booking');
    });

    it('generates a request ID for messages published before IDs existed', async () => {
        const { handleSeatRestoration, logger } = load(async () => {});

        await handleSeatRestoration(fakeChannel(), message(event));

        assert.match(logger.records[0].requestId, /^[0-9a-f-]{36}$/);
    });
});

describe('booking.confirmed consumer (Notifications Service)', () => {
    function load(sendEmail) {
        const requestContext = require(servicePath('Notifications-Service/src/utils/request-context.js'));
        const logger = createRecordingLogger(requestContext.getRequestId);
        class Resend {
            constructor() {
                this.emails = { send: sendEmail };
            }
        }
        const { handleBookingConfirmed } = loadWithMocks('Notifications-Service', 'src/subscribers/booking-confirmed.js', [
            ['resend', { Resend }],
            [servicePath('Notifications-Service/src/config/index.js'), {
                ServerConfig: { RESEND_API_KEY: 're_test', FROM_EMAIL: 'test@example.com' },
                RabbitMQ: {},
                Logger: logger,
            }],
            [servicePath('Notifications-Service/src/utils/request-context.js'), requestContext],
        ]);
        return { handleBookingConfirmed, logger };
    }

    const event = { bookingId: 9, userId: 1, email: 'user@example.com', flightId: 3, totalCost: 4500 };

    it('sends the email and acks', async () => {
        const sent = [];
        const { handleBookingConfirmed } = load(async (email) => { sent.push(email); return { data: { id: 'e1' } }; });
        const channel = fakeChannel();

        await handleBookingConfirmed(channel, message(event));

        assert.equal(sent.length, 1);
        assert.equal(sent[0].to, 'user@example.com');
        assert.equal(channel.acks.length, 1);
    });

    it('discards a malformed message instead of redelivering it forever', async () => {
        const { handleBookingConfirmed } = load(async () => assert.fail('must not send'));
        const channel = fakeChannel();

        await handleBookingConfirmed(channel, message('{not json'));

        assert.deepEqual(channel.nacks.map((n) => n.requeue), [false]);
    });

    it('discards a message with no email address', async () => {
        const { handleBookingConfirmed } = load(async () => assert.fail('must not send'));
        const channel = fakeChannel();

        await handleBookingConfirmed(channel, message({ ...event, email: undefined }));

        assert.deepEqual(channel.nacks.map((n) => n.requeue), [false]);
    });

    it('discards (does not requeue) when the email provider throws', async () => {
        const { handleBookingConfirmed, logger } = load(async () => { throw new Error('fetch failed'); });
        const channel = fakeChannel();

        await handleBookingConfirmed(channel, message(event));

        assert.deepEqual(channel.nacks.map((n) => n.requeue), [false]);
        assert.ok(logger.records.some((r) => r.level === 'error' && r.meta.bookingId === 9));
    });

    it('treats an error result from the email provider as a failure', async () => {
        const { handleBookingConfirmed } = load(async () => ({ error: { message: 'API key is invalid' } }));
        const channel = fakeChannel();

        await handleBookingConfirmed(channel, message(event));

        assert.equal(channel.acks.length, 0);
        assert.equal(channel.nacks.length, 1);
    });

    it('logs under the request ID of the payment that published the event', async () => {
        const { handleBookingConfirmed, logger } = load(async () => ({ data: {} }));

        await handleBookingConfirmed(fakeChannel(), message(event, { requestId: 'req-payment-1' }));

        assert.ok(logger.records.length > 0);
        for (const record of logger.records) assert.equal(record.requestId, 'req-payment-1');
    });

    it('does not write the customer\'s email address into the logs', async () => {
        const { handleBookingConfirmed, logger } = load(async () => { throw new Error('fetch failed'); });

        await handleBookingConfirmed(fakeChannel(), message(event));

        assert.ok(!JSON.stringify(logger.records).includes('user@example.com'));
    });
});
