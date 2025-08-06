import { createRedisPubSubEmitter } from "./create-redis-event-emitter";
import { RedisClient } from "bun";
import { createClient, RedisClientType } from "redis";
import { describe, it, expect, beforeAll, afterAll } from "bun:test";

const TEST_CHANNELS = ["test-channel-1"];
const TEST_MESSAGE = "hello world";
const REDIS_URL = "redis://127.0.0.1:6379";

describe("createRedisPubSubEmitter", () => {
    let nodeRedis: RedisClientType;
    let nodeRedisSubscriber: RedisClientType;

    beforeAll(async () => {
        nodeRedis = createClient({ url: REDIS_URL });
        nodeRedisSubscriber = createClient({ url: REDIS_URL });
        await nodeRedis.connect();
        await nodeRedisSubscriber.connect();
    });

    afterAll(async () => {
        await nodeRedis.quit();
        await nodeRedisSubscriber.quit();
    });

    it("should write and read data using redis package client", async () => {
        const emitter = await createRedisPubSubEmitter(TEST_CHANNELS, nodeRedisSubscriber, nodeRedis);

        const receivedPromise = new Promise<string>((resolve) => {
            emitter.events.on('test-channel-1', (message: string) => {
                console.log("Received message:", message);
                resolve(message);
            });
        });

        await emitter.publish(TEST_MESSAGE);

        const received = await receivedPromise;
        expect(received).toBe(TEST_MESSAGE);

        await emitter.close();
    });
});
