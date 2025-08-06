import { RedisClient } from "bun";
import { RedisClientType } from "redis";
import { EventEmitter } from "events";

const notConnectedError = new Error("Redis client is not connected. Please ensure the client is connected before creating a stream.");
const emptyChannelsError = new Error("Channels must be a non-empty array.");

async function createRedisPubSubEmitter(
    channels: string[], 
    subscriber: RedisClientType, 
    publisher: RedisClientType
) {
    if (!Array.isArray(channels) || channels.length === 0) {
        throw emptyChannelsError;
    }

    const eventEmitter = new EventEmitter();

    if (!subscriber.isOpen || !subscriber.isReady) {
        throw notConnectedError;
    }

    if (publisher instanceof RedisClient || !publisher.isOpen || !publisher.isReady) {
        throw new Error("Publisher Redis client is not a valid or connected node-redis client.");
    }

    try {
        await subscriber.subscribe(channels, (message, channel) => {
            eventEmitter.emit(channel, message);
        });
    } catch (error) {
        eventEmitter.emit('error', error);
        throw error;
    }

    const publish = async (message: string, targetChannel?: string) => {
        const channelsToPublish = targetChannel ? [targetChannel] : channels;
        for (const channel of channelsToPublish) {
            await publisher.publish(channel, message);
        }
    };

    const close = async () => {
        await subscriber.unsubscribe(channels);
        eventEmitter.removeAllListeners();
    };

    return {
        events: eventEmitter,
        publish,
        close
    };
}

export { createRedisPubSubEmitter };
