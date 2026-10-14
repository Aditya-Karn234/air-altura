/*
 * Logger
 *
 * Writes one JSON object per line to stdout, e.g.
 *   {"level":"info","message":"request completed","service":"api-gateway","requestId":"…","timestamp":"…"}
 *
 * - requestId is filled in from the request context, so callers never pass it by hand.
 * - Error values in metadata ({ err }) are expanded to { message, stack } instead of {}.
 * - LOG_LEVEL controls verbosity (default: info).
 */

const { createLogger, format, transports } = require('winston');
const { getRequestId } = require('../utils/request-context');

const SERVICE_NAME = 'notifications-service';

const addRequestId = format((info) => {
    const requestId = getRequestId();
    if (requestId && !info.requestId) info.requestId = requestId;
    return info;
});

const serializeErrors = format((info) => {
    for (const [key, value] of Object.entries(info)) {
        if (value instanceof Error) {
            info[key] = { name: value.name, message: value.message, statusCode: value.statusCode, stack: value.stack };
        }
    }
    return info;
});

const logger = createLogger({
    level: process.env.LOG_LEVEL || 'info',
    defaultMeta: { service: SERVICE_NAME },
    format: format.combine(
        addRequestId(),
        serializeErrors(),
        format.timestamp(),
        format.json()
    ),
    transports: [new transports.Console()],
});

module.exports = logger;
