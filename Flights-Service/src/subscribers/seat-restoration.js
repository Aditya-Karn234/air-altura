/*
 * Seat Restoration Subscriber
 *
 * Listens to the 'seat.restoration' queue published by the Booking Service cron job
 * whenever bookings are cancelled due to payment timeout.
 *
 * For each message:
 *  1. Parse the event: { bookingId, flightId, seats, seatClass }
 *  2. Call FlightService.updateSeats — routes to FlightClasses when seatClass is present,
 *     falls back to Flights.totalSeats for pre-v2 messages that lack a seatClass field.
 *  3. Acknowledge the message (ack) — RabbitMQ removes it from the queue
 *
 * On failure (Flight Service DB error etc.):
 *  - nack the message with requeue: true
 *  - RabbitMQ puts it back in the queue for retry
 *  - This guarantees seats are eventually restored even if something goes wrong
 *
 * Why this beats a direct HTTP call from the cron job:
 *  - If this service is down when cron fires, messages wait in the durable queue
 *  - When this service restarts, it processes all waiting messages automatically
 *  - No seats are permanently lost
 */

const { RabbitMQ, Logger } = require('../config');
const { FlightService } = require('../services');
const { REQUEST_ID_HEADER, resolveRequestId, runWithRequestId } = require('../utils/request-context');

/*
 * handleSeatRestoration
 *
 * Processes one message. Runs inside the request ID carried in the message headers,
 * so these logs share an ID with the booking request that cancelled the seats.
 */
async function handleSeatRestoration(channel, message) {
    const requestId = resolveRequestId(message.properties.headers?.[REQUEST_ID_HEADER]);

    return runWithRequestId(requestId, async () => {
        try {
            const { bookingId, flightId, seats, seatClass } = JSON.parse(message.content.toString());
            Logger.info('Restoring seats', { bookingId, flightId, seats, seatClass, redelivered: message.fields.redelivered });

            await FlightService.updateSeats({ flightId, seats, dec: false, seatClass });

            // ack — tell RabbitMQ this message was processed successfully, remove from queue
            channel.ack(message);
        } catch (error) {
            Logger.error('Seat restoration failed, requeueing message', { err: error });
            // nack with requeue:true — put message back in queue for retry
            channel.nack(message, false, true);
        }
    });
}

async function subscribeSeatRestoration() {
    // registerConsumer re-runs this after every RabbitMQ reconnect
    await RabbitMQ.registerConsumer(async (channel) => {
        await channel.consume('seat.restoration', (message) => {
            if (!message) return;
            return handleSeatRestoration(channel, message);
        });
        Logger.info('Seat restoration subscriber active');
    });
}

module.exports = subscribeSeatRestoration;
module.exports.handleSeatRestoration = handleSeatRestoration;
