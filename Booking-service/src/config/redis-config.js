const Redis = require('ioredis');
const Logger = require('./logger-config');

const client = process.env.REDIS_URL
    ? new Redis(process.env.REDIS_URL)
    : new Redis({
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT) || 6379,
    });

client.on('connect', () => Logger.info('Redis connected'));
client.on('error', (err) => Logger.error('Redis connection error', { err }));

module.exports = client;
