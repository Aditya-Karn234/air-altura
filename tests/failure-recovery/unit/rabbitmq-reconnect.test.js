/*
 * RabbitMQ connection recovery — what each service does when the broker restarts or
 * the connection drops, using a fake amqplib so connections can be killed on demand.
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const { loadWithMocks, servicePath, createRecordingLogger, waitFor } = require('../helpers/load-with-mocks');

class FakeChannel extends EventEmitter {
    constructor(connection) {
        super();
        this.connection = connection;
        this.queues = [];
        this.consumers = [];
        this.sent = [];
    }
    async assertQueue(name, options) { this.queues.push({ name, options }); }
    async prefetch() {}
    async consume(queue, handler) { this.consumers.push({ queue, handler }); }
    sendToQueue(queue, content, options) {
        if (this.connection.closed) throw new Error('Channel closed');
        this.sent.push({ queue, payload: JSON.parse(content.toString()), options });
        return true;
    }
}

class FakeConnection extends EventEmitter {
    constructor() {
        super();
        this.channels = [];
        this.closed = false;
    }
    async createChannel() {
        const channel = new FakeChannel(this);
        this.channels.push(channel);
        return channel;
    }
    async close() {
        if (this.closed) throw new Error('Connection closed');
        this.kill();
    }
    // Broker went away: amqplib emits 'error' (e.g. CONNECTION_FORCED) and then 'close'
    kill({ withError = true } = {}) {
        this.closed = true;
        if (withError) this.emit('error', new Error('Connection closed: 320 (CONNECTION-FORCED)'));
        for (const channel of this.channels) channel.emit('close');
        this.emit('close');
    }
}

function fakeAmqp() {
    const amqp = {
        connections: [],
        brokerDown: false,
        connectAttempts: 0,
        async connect() {
            amqp.connectAttempts++;
            if (amqp.brokerDown) throw new Error('connect ECONNREFUSED 127.0.0.1:5672');
            const connection = new FakeConnection();
            amqp.connections.push(connection);
            return connection;
        },
        get current() {
            return amqp.connections[amqp.connections.length - 1];
        },
    };
    return amqp;
}

const SERVICES = [
    { dir: 'Booking-service', queues: ['seat.restoration', 'booking.confirmed'] },
    { dir: 'Flights-Service', queues: ['seat.restoration'] },
    { dir: 'Notifications-Service', queues: ['booking.confirmed'] },
];

function load(dir) {
    const amqp = fakeAmqp();
    const requestContext = require(servicePath(dir, 'src/utils/request-context.js'));
    const logger = createRecordingLogger(requestContext.getRequestId);
    const rabbit = loadWithMocks(dir, 'src/config/rabbitmq-config.js', [
        ['amqplib', amqp],
        [servicePath(dir, 'src/config/logger-config.js'), logger],
        [servicePath(dir, 'src/utils/request-context.js'), requestContext],
        ...(dir === 'Notifications-Service'
            ? [[servicePath(dir, 'src/config/server-config.js'), { RABBITMQ_URL: 'amqp://fake' }]]
            : []),
    ]);
    return { rabbit, amqp, logger, requestContext };
}

before(() => { process.env.RABBITMQ_RECONNECT_DELAY_MS = '10'; });
after(() => { delete process.env.RABBITMQ_RECONNECT_DELAY_MS; });

for (const { dir, queues } of SERVICES) {
    describe(`RabbitMQ reconnect (${dir})`, () => {
        it('asserts its queues as durable on connect', async () => {
            const { rabbit, amqp } = load(dir);
            await rabbit.connectRabbitMQ();

            const asserted = amqp.current.channels[0].queues;
            assert.deepEqual(asserted.map((q) => q.name), queues);
            assert.ok(asserted.every((q) => q.options.durable === true));
        });

        it('survives a broker restart without crashing and reconnects once', async () => {
            const { rabbit, amqp } = load(dir);
            await rabbit.connectRabbitMQ();

            // Would throw "Unhandled 'error' event" if no error listener were attached
            amqp.current.kill();
            await waitFor(() => amqp.connections.length === 2 && rabbit.getChannel(), { description: 'reconnect' });

            await new Promise((resolve) => setTimeout(resolve, 50));
            assert.equal(amqp.connections.length, 2, "'error' + 'close' for one failure must not open two connections");
        });

        it('re-registers consumers on the new channel after reconnecting', async () => {
            const { rabbit, amqp } = load(dir);
            await rabbit.connectRabbitMQ();

            let setups = 0;
            await rabbit.registerConsumer(async (channel) => {
                setups++;
                await channel.consume(queues[0], () => {});
            });
            assert.equal(amqp.current.channels[0].consumers.length, 1);

            amqp.current.kill();
            await waitFor(() => amqp.connections.length === 2 && rabbit.getChannel(), { description: 'reconnect' });

            assert.equal(setups, 2);
            assert.equal(amqp.current.channels[0].consumers.length, 1, 'new channel must have its consumer back');
        });

        it('keeps retrying while the broker is still down, then recovers', async () => {
            const { rabbit, amqp } = load(dir);
            await rabbit.connectRabbitMQ();

            amqp.brokerDown = true;
            amqp.current.kill();
            await waitFor(() => amqp.connectAttempts >= 4, { description: 'several failed reconnect attempts' });
            assert.throws(() => rabbit.getChannel(), /not initialized/);

            amqp.brokerDown = false;
            await waitFor(() => { try { return rabbit.getChannel(); } catch { return false; } }, { description: 'recovery' });
        });

        it('rebuilds the connection when only the channel closes', async () => {
            const { rabbit, amqp } = load(dir);
            await rabbit.connectRabbitMQ();

            amqp.current.channels[0].emit('close');
            await waitFor(() => amqp.connections.length === 2 && rabbit.getChannel(), { description: 'reconnect' });
        });
    });
}

describe('RabbitMQ publish (Booking-service)', () => {
    it('sends persistent messages stamped with the current request ID', async () => {
        const { rabbit, amqp, requestContext } = load('Booking-service');
        await rabbit.connectRabbitMQ();

        requestContext.runWithRequestId('req-123', () => rabbit.publish('booking.confirmed', { bookingId: 1 }));

        const [sent] = amqp.current.channels[0].sent;
        assert.equal(sent.queue, 'booking.confirmed');
        assert.deepEqual(sent.payload, { bookingId: 1 });
        assert.equal(sent.options.persistent, true);
        assert.equal(sent.options.headers['x-request-id'], 'req-123');
    });

    it('throws while disconnected (callers log it) and publishes again after reconnect', async () => {
        const { rabbit, amqp } = load('Booking-service');
        await rabbit.connectRabbitMQ();

        amqp.brokerDown = true;
        amqp.current.kill();
        assert.throws(() => rabbit.publish('seat.restoration', { bookingId: 2 }), /not initialized/);

        amqp.brokerDown = false;
        await waitFor(() => amqp.connections.length === 2, { description: 'reconnect' });
        await waitFor(() => { try { return rabbit.getChannel(); } catch { return false; } }, { description: 'channel' });

        rabbit.publish('seat.restoration', { bookingId: 2 });
        assert.equal(amqp.current.channels[0].sent.length, 1);
    });
});
