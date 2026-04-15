"use strict";
const Redis = require("ioredis");

const redis = new Redis({
    host: "127.0.0.1",
    port: 6379,
    maxRetriesPerRequest: 1,
    retryStrategy(times) {
        if (times > 3) return null;
        return Math.min(times * 500, 2000);
    },
    lazyConnect: false,
    enableOfflineQueue: false,
});

let isReady = false;

redis.on("ready", () => { isReady = true; console.log("[redis] Connected"); });
redis.on("error", () => { isReady = false; });
redis.on("close", () => { isReady = false; });

async function cacheGet(key) {
    if (!isReady) return null;
    try {
        const raw = await redis.get(key);
        return raw ? JSON.parse(raw) : null;
    } catch {
        return null;
    }
}

async function cacheSet(key, value, ttlSeconds) {
    if (!isReady) return;
    try {
        await redis.set(key, JSON.stringify(value), "EX", ttlSeconds);
    } catch {}
}

async function cacheDel(key) {
    if (!isReady) return;
    try {
        await redis.del(key);
    } catch {}
}

module.exports = { redis, cacheGet, cacheSet, cacheDel };
