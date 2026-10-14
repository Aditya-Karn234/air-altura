const { Router } = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');
const { authenticate } = require('../../middlewares/auth-middleware');
const { rateLimiter } = require('../../middlewares/rate-limiter');
const authRouter = require('./auth-router');
const { FLIGHTS_SERVICE, BOOKING_SERVICE } = require('../../config/server-config');
const logger = require('../../config/logger-config');

const router = Router();

router.use('/auth', authRouter);

// Express strips the mounted prefix from req.url before the proxy sees it.
// Reuse the original incoming path so downstream services receive the versioned route.
const restorePath = {
    pathRewrite: (_path, req) => req.originalUrl,
};

// Log upstream failures (service down, connection reset) with the request ID.
// Added as a plugin rather than `on.error`, because `on.error` would disable
// http-proxy-middleware's default 502/504 error response.
function logProxyErrors(target) {
    return {
        plugins: [(proxyServer) => {
            proxyServer.on('error', (err, req) => {
                logger.error('Proxy request failed', { requestId: req?.id, target, path: req?.originalUrl, err });
            });
        }],
    };
}

const flightsProxy = createProxyMiddleware({
    target: FLIGHTS_SERVICE,
    changeOrigin: true,
    ...restorePath,
    ...logProxyErrors(FLIGHTS_SERVICE),
});

router.use('/flights', rateLimiter, flightsProxy);
router.use('/airplanes', rateLimiter, flightsProxy);
router.use('/airports', rateLimiter, flightsProxy);
router.use('/cities', rateLimiter, flightsProxy);

const bookingProxy = createProxyMiddleware({
    target: BOOKING_SERVICE,
    changeOrigin: true,
    ...restorePath,
    ...logProxyErrors(BOOKING_SERVICE),
});

router.use('/booking', rateLimiter, authenticate, bookingProxy);

module.exports = router;
