const express = require('express');
const cors = require('cors');
const { PORT } = require('./config/server-config');
const logger = require('./config/logger-config');
const { sequelize } = require('./models');
const { requestLogger } = require('./middlewares/request-logger');
const apiRoutes = require('./routes');

const app = express();

// ALLOWED_ORIGIN: set to your Vercel URL in production (e.g. https://air-altura.vercel.app)
// Falls back to * so local dev and testing work without configuration.
app.use(cors({
    origin: process.env.ALLOWED_ORIGIN || '*',
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'Idempotency-Key', 'X-Request-Id'],
    exposedHeaders: ['X-Request-Id'],
}));

// Body parsing only applied to /api/v1/auth - routes the Gateway handles directly.
// Proxy routes (/flights, /bookings etc.) must NOT have body parsed here,
// as consuming the stream prevents http-proxy-middleware from forwarding the body.
app.use('/api/v1/auth', express.json());
app.use('/api/v1/auth', express.urlencoded({ extended: true }));

// Registered after the body parsers so their stream callbacks can't drop the request context
app.use(requestLogger);

app.use('/api', apiRoutes);

app.listen(PORT, async () => {
    logger.info('API Gateway started', { port: Number(PORT) });
    try {
        await sequelize.authenticate();
        logger.info('DB connected');
    } catch (err) {
        logger.error('DB connection failed', { err });
    }
});
