/*
 * Request Context
 *
 * Holds the current request ID in AsyncLocalStorage so any code running on behalf of a
 * request (controllers, Redis calls, proxy hooks) can read it without it being passed
 * through every function. The logger reads it automatically.
 *
 * The same header name is used by every service, so one ID follows a request from the
 * gateway into the booking service, the flights service, and RabbitMQ message headers.
 */

const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID } = require('node:crypto');

const REQUEST_ID_HEADER = 'x-request-id';

// Accept caller-supplied IDs only when they look sane, so a client can't push
// huge or multi-line values into our logs.
const VALID_REQUEST_ID = /^[A-Za-z0-9._:-]{1,128}$/;

const storage = new AsyncLocalStorage();

function resolveRequestId(incoming) {
    return typeof incoming === 'string' && VALID_REQUEST_ID.test(incoming) ? incoming : randomUUID();
}

function runWithRequestId(requestId, fn) {
    return storage.run({ requestId }, fn);
}

function getRequestId() {
    return storage.getStore()?.requestId;
}

module.exports = { REQUEST_ID_HEADER, resolveRequestId, runWithRequestId, getRequestId };
