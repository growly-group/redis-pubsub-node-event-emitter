import { createRedisPubSubStream } from "./create-redis-stream";
import { RedisClient } from "bun";
import { createClient, RedisClientType } from "redis";
import { describe, it, expect, beforeAll, afterAll } from "bun:test";

const TEST_CHANNELS = ["test-channel-1", "test-channel-2"];
const TEST_MESSAGE = "hello world";
const REDIS_URL = "redis://127.0.0.1:6379";

describe("createRedisPubSubStream", () => {
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
        const stream = await createRedisPubSubStream(TEST_CHANNELS, nodeRedisSubscriber, nodeRedis);

        const receivedPromise = new Promise<string>((resolve) => {
            stream[1].getReader().read().then(({ value }) => {
                resolve(value ?? "");
            });
        });

        await new Promise<void>((resolve, reject) => {
            stream[0].getWriter().write(TEST_MESSAGE).then(() => {
                resolve();
            }).catch((err) => {
                reject(err);
            });
        });

        const received = await receivedPromise;
        expect(received).toContain(TEST_MESSAGE);
    });
});
