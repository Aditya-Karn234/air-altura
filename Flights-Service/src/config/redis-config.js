const Redis = require('ioredis');
const Logger = require('./logger-config');

const redis = process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL, { lazyConnect: true })
    : new Redis({
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT) || 6379,
        lazyConnect: true,
    });

redis.on('connect', () => Logger.info('Redis connected'));
redis.on('error', (err) => Logger.error('Redis error', { err }));

module.exports = redis;
