const Redis = require('ioredis');
const { REDIS_HOST, REDIS_PORT } = require('./server-config');
const logger = require('./logger-config');

const redis = process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL, { lazyConnect: true })
    : new Redis({
        host: REDIS_HOST,
        port: REDIS_PORT,
        lazyConnect: true,
    });

redis.on('connect', () => logger.info('Redis connected'));
redis.on('error', (err) => logger.error('Redis error', { err }));

module.exports = redis;
