
/*
 * Entry point of the Booking Service.
 *
 * Responsibilities:
 *  - Spins up the Express server on the configured PORT
 *  - Mounts all API routes under /api
 *  - Starts all background cron jobs AFTER the server is live
 *
 * Why cron is started inside app.listen callback:
 *  - Ensures all modules are fully loaded before the cron fires its first tick,
 *    avoiding any potential race conditions with module initialization.
 */

const express = require('express');
const { ServerConfig, Logger, RabbitMQ } = require('./config');
const apiRoutes = require('./routes');
const CRON = require('./utils/common/cron-jobs');
const { requestLogger } = require('./middlewares');
const app = express();

// Parse incoming JSON bodies (for raw JSON requests)
app.use(express.json())
// Parse URL-encoded bodies (for form submissions / x-www-form-urlencoded)
app.use(express.urlencoded({extended:true}));

// Picks up the X-Request-Id forwarded by the API Gateway and logs every request.
// Registered after the body parsers so their stream callbacks can't drop the request context.
app.use(requestLogger);

// Mount all versioned API routes under /api  →  /api/v1/...
app.use('/api', apiRoutes)

app.listen(ServerConfig.PORT, async () => {
    Logger.info('Booking Service started', { port: Number(ServerConfig.PORT) });

    await RabbitMQ.connectRabbitMQ();

    // Start background cron jobs after RabbitMQ is connected
    // cron needs getChannel() to publish seat restoration events
    CRON();
})