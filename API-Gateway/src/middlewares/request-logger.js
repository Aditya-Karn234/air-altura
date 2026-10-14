const logger = require('../config/logger-config');
const { REQUEST_ID_HEADER, resolveRequestId, runWithRequestId } = require('../utils/request-context');

/*
 * requestLogger
 *
 * - Reuses the caller's X-Request-Id or generates one.
 * - Writes it back onto req.headers so the proxy forwards it to downstream services.
 * - Echoes it in the response so a user-reported failure can be traced in the logs.
 * - Logs one line per completed request with status and duration.
 */
function requestLogger(req, res, next) {
    const requestId = resolveRequestId(req.headers[REQUEST_ID_HEADER]);
    req.id = requestId;
    req.headers[REQUEST_ID_HEADER] = requestId;
    res.setHeader('X-Request-Id', requestId);

    const startedAt = process.hrtime.bigint();
    res.on('finish', () => {
        const durationMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
        const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';
        // 'finish' fires outside the request's async context, so pass requestId explicitly
        logger.log(level, 'request completed', {
            requestId,
            method: req.method,
            path: req.originalUrl,
            status: res.statusCode,
            durationMs: Math.round(durationMs * 10) / 10,
            userId: req.headers['x-user-id'],
        });
    });

    runWithRequestId(requestId, next);
}

module.exports = { requestLogger };
