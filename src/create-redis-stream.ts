import { RedisClient } from "bun";
import { RedisClientType } from "redis";
import { EventEmitter } from "events";

const notConnectedError = new Error("Redis client is not connected. Please ensure the client is connected before creating a stream.");
const emptyChannelsError = new Error("Channels must be a non-empty array.");

async function createRedisPubSubStream(
    channels: string[], 
    subscriber: RedisClientType, 
    publisher: RedisClientType
) {
    if (!Array.isArray(channels) || channels.length === 0) {
        throw emptyChannelsError;
    }

    let readStream: ReadableStream<string> | undefined;
    let writeStream: WritableStream<string> | undefined;
    const eventEmitter = new EventEmitter();

    if (!subscriber.isOpen || !subscriber.isReady) {
            throw notConnectedError;
        }
        if (publisher instanceof RedisClient || !publisher.isOpen || !publisher.isReady) {
            throw new Error("Publisher Redis client is not a valid or connected node-redis client.");
        }

        readStream = new ReadableStream<string>({
            async start(controller) {
                try {
                    await subscriber.subscribe(channels, (message, channel) => {
                        if (eventEmitter.listenerCount(channel) > 0) {
                            eventEmitter.emit(channel, message);
                        }
                        controller.enqueue(`${channel},${message}`);
                    });
                } catch (error) {
                    eventEmitter.emit('close', error);
                    throw error;
                }
            },
            async cancel() {
                eventEmitter.emit('close', null);
                eventEmitter.removeAllListeners();
                await subscriber.unsubscribe(channels);
            }
        });

        writeStream = new WritableStream<string>({
            async write(chunk) {
                for (const channel of channels) {
                    await publisher.publish(channel, chunk);
                }
            },
            async close() {
                await subscriber.unsubscribe(channels);
            }
        });

    return {
        readStream,
        writeStream,
        event: eventEmitter
    }
}

export { createRedisPubSubStream };