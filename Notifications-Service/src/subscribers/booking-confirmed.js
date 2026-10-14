const { Resend } = require('resend');
const { ServerConfig, RabbitMQ, Logger } = require('../config');
const { REQUEST_ID_HEADER, resolveRequestId, runWithRequestId } = require('../utils/request-context');

const resend = new Resend(ServerConfig.RESEND_API_KEY);

/*
 * subscribeBookingConfirmed
 *
 * Consumes messages from the 'booking.confirmed' queue published by the Booking Service
 * after a successful payment. For each message:
 *
 *  1. Parse the event payload { bookingId, userId, email, flightId, totalCost }
 *  2. Send a booking confirmation email via Resend — email comes from the payload
 *     (forwarded by the API Gateway from the JWT so no DB lookup needed)
 *  3. ack the message on success so RabbitMQ removes it from the queue
 *  4. nack (requeue: false) on failure — logs the error, discards the message
 *     to avoid infinite retry loops on bad payloads.
 *
 * prefetch(1): process one message at a time so a slow email send
 * doesn't cause unbounded memory growth from queued messages.
 */
async function subscribeBookingConfirmed() {
    // registerConsumer re-runs this after every RabbitMQ reconnect
    await RabbitMQ.registerConsumer(async (channel) => {
        // Process one message at a time
        await channel.prefetch(1);

        await channel.consume('booking.confirmed', (msg) => {
            if (!msg) return;
            return handleBookingConfirmed(channel, msg);
        });

        Logger.info('Subscribed to booking.confirmed queue');
    });
}

/*
 * handleBookingConfirmed
 *
 * Processes one message. Runs inside the request ID carried in the message headers,
 * so these logs share an ID with the payment request that published the event.
 */
function handleBookingConfirmed(channel, msg) {
    const requestId = resolveRequestId(msg.properties.headers?.[REQUEST_ID_HEADER]);
    return runWithRequestId(requestId, () => processBookingConfirmed(channel, msg));
}

async function processBookingConfirmed(channel, msg) {
    let payload;
    try {
        payload = JSON.parse(msg.content.toString());
    } catch (err) {
        Logger.error('Failed to parse booking.confirmed message, discarding', { err });
        channel.nack(msg, false, false);
        return;
    }

    const { bookingId, email, flightId, totalCost } = payload;

    if (!email) {
        Logger.error('booking.confirmed has no email, discarding', { bookingId });
        channel.nack(msg, false, false);
        return;
    }

    try {
        // Send confirmation email via Resend
        const result = await resend.emails.send({
            from: ServerConfig.FROM_EMAIL,
            to: email,
            subject: 'Your booking is confirmed!',
            html: `
                <h2>Booking Confirmed</h2>
                <p>Hi, your flight booking has been confirmed.</p>
                <ul>
                    <li><strong>Booking ID:</strong> ${bookingId}</li>
                    <li><strong>Flight ID:</strong> ${flightId}</li>
                    <li><strong>Amount Paid:</strong> ₹${totalCost}</li>
                </ul>
                <p>Have a great flight!</p>
            `,
        });

        if (result?.error) {
            throw new Error(result.error.message || 'Unknown Resend error');
        }

        Logger.info('Confirmation email sent', { bookingId });
        channel.ack(msg);
    } catch (err) {
        // nack without requeue — avoid infinite loops on persistent failures
        // (e.g. Resend API down, invalid email). The message is discarded.
        Logger.error('Failed to send confirmation email, discarding message', { bookingId, err });
        channel.nack(msg, false, false);
    }
}

module.exports = { subscribeBookingConfirmed, handleBookingConfirmed };
