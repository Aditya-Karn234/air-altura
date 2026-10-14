/*
 * RabbitMQ Config — Flights Service
 *
 * Manages a single shared connection and channel for the entire service.
 * The seat restoration subscriber registers itself with registerConsumer().
 *
 * Queues asserted here:
 *   - seat.restoration : published by Booking Service, consumed here to restore seats
 *
 * assertQueue is idempotent — safe to call on both publisher and consumer side.
 * Whichever service starts first will create the queue; the other just connects to it.
 *
 * Reconnection:
 *   When the connection drops (broker restart, network blip), the service reconnects
 *   every RECONNECT_DELAY_MS, re-asserts the queue and re-runs registered consumers,
 *   so seat restoration resumes on its own. Unacked messages are redelivered by RabbitMQ.
 */

const amqp = require('amqplib');
const Logger = require('./logger-config');
const { REQUEST_ID_HEADER, getRequestId } = require('../utils/request-context');

const QUEUES = ['seat.restoration'];
const RECONNECT_DELAY_MS = parseInt(process.env.RABBITMQ_RECONNECT_DELAY_MS) || 5000;

let channel = null;
let reconnectTimer = null;
const consumers = [];

async function connectRabbitMQ() {
    const connection = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');

    // Without an 'error' listener, a broker shutdown would crash the process.
    // The 'close' event always follows, and that's where we reconnect.
    connection.on('error', (err) => Logger.error('RabbitMQ connection error', { err }));
    connection.on('close', () => {
        channel = null;
        Logger.warn('RabbitMQ connection closed, reconnecting', { delayMs: RECONNECT_DELAY_MS });
        scheduleReconnect();
    });

    let newChannel;
    try {
        newChannel = await connection.createChannel();
        newChannel.on('error', (err) => Logger.error('RabbitMQ channel error', { err }));
        // A channel can close on its own (e.g. protocol error). Close the connection too,
        // so the 'close' handler above rebuilds everything from scratch.
        newChannel.on('close', () => connection.close().catch(() => {}));

        for (const queue of QUEUES) {
            await newChannel.assertQueue(queue, { durable: true });
        }
        for (const setupConsumer of consumers) {
            await setupConsumer(newChannel);
        }
    } catch (err) {
        // Don't leak a half-initialised connection; its 'close' handler schedules the retry
        connection.close().catch(() => {});
        throw err;
    }

    channel = newChannel;
    Logger.info('RabbitMQ connected', { queues: QUEUES });
    return channel;
}

function scheduleReconnect() {
    if (reconnectTimer) return;  // 'error' and 'close' can both fire for one failure
    reconnectTimer = setTimeout(async () => {
        reconnectTimer = null;
        try {
            await connectRabbitMQ();
        } catch (err) {
            Logger.error('RabbitMQ reconnect failed', { err });
            scheduleReconnect();
        }
    }, RECONNECT_DELAY_MS);
}

/*
 * registerConsumer
 *
 * setupConsumer(channel) is run now (if connected) and again after every reconnect,
 * so consumers don't silently stop after a broker restart.
 */
async function registerConsumer(setupConsumer) {
    consumers.push(setupConsumer);
    if (channel) await setupConsumer(channel);
}

function getChannel() {
    if (!channel) throw new Error('RabbitMQ channel not initialized — call connectRabbitMQ first');
    return channel;
}

/*
 * publish
 *
 * Sends a persistent JSON message and stamps it with the current request ID,
 * so the consumer's logs can be joined with the HTTP request that caused it.
 */
function publish(queue, payload) {
    const requestId = getRequestId();
    return getChannel().sendToQueue(queue, Buffer.from(JSON.stringify(payload)), {
        persistent: true,  // message survives RabbitMQ restart
        contentType: 'application/json',
        headers: requestId ? { [REQUEST_ID_HEADER]: requestId } : {},
    });
}

module.exports = { connectRabbitMQ, registerConsumer, getChannel, publish };
