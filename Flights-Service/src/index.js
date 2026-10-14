
const express = require('express');
const { ServerConfig, Logger, RabbitMQ } = require('./config');
const apiRoutes = require('./routes');
const subscribeSeatRestoration = require('./subscribers/seat-restoration');
const { requestLogger } = require('./middlewares/request-logger');

const app = express();
app.use(express.json());
app.use(express.urlencoded({extended: true}))
// Picks up the X-Request-Id forwarded by the API Gateway / Booking Service and logs every request
app.use(requestLogger);
app.use('/api', apiRoutes)

app.listen(ServerConfig.PORT, async () => {
    Logger.info('Flights Service started', { port: Number(ServerConfig.PORT) });

    await RabbitMQ.connectRabbitMQ();
    await subscribeSeatRestoration();
})