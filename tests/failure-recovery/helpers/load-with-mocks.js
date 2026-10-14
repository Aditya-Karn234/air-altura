/*
 * Loads a service module with some of its dependencies replaced by fakes.
 *
 * Each call gets a fresh copy of the target module (and of anything it requires that
 * isn't mocked), so module-level state such as RabbitMQ connections never leaks between tests.
 */

const path = require('node:path');

const ROOT = path.resolve(__dirname, '../../..');

function servicePath(...segments) {
    return path.join(ROOT, ...segments);
}

/*
 * mocks: array of [specifier, exports]
 *   specifier: an absolute file path, or a package name resolved from `serviceDir`
 */
function loadWithMocks(serviceDir, modulePath, mocks = []) {
    const serviceRoot = servicePath(serviceDir);
    const resolve = (spec) => require.resolve(spec, { paths: [serviceRoot] });

    // Drop any cached copies of this service's own source files so module state starts fresh
    const sourceDir = path.join(serviceRoot, 'src') + path.sep;
    for (const id of Object.keys(require.cache)) {
        if (id.startsWith(sourceDir)) delete require.cache[id];
    }

    for (const [spec, exports] of mocks) {
        const id = resolve(spec);
        require.cache[id] = { id, filename: id, loaded: true, exports };
    }

    const target = resolve(path.join(serviceRoot, modulePath));
    try {
        return require(target);
    } finally {
        for (const [spec] of mocks) delete require.cache[resolve(spec)];
    }
}

// Logger stand-in that records each call along with the request ID active at the time
function createRecordingLogger(getRequestId) {
    const records = [];
    const record = (level) => (message, meta = {}) => {
        records.push({ level, message, meta, requestId: getRequestId ? getRequestId() : undefined });
    };
    return {
        records,
        info: record('info'),
        warn: record('warn'),
        error: record('error'),
        debug: record('debug'),
        log: (level, message, meta) => record(level)(message, meta),
    };
}

async function waitFor(condition, { timeoutMs = 2000, intervalMs = 5, description = 'condition' } = {}) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (await condition()) return;
        await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
    throw new Error(`Timed out after ${timeoutMs}ms waiting for ${description}`);
}

module.exports = { ROOT, servicePath, loadWithMocks, createRecordingLogger, waitFor };
