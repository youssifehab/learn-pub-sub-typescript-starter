import amqp from "amqplib";
import { declareAndBind, type SimpleQueueType } from "./declareAndBind.js";

export enum AckType {
  Ack = "Ack",
  NackQueue = "NackQueue",
  NackDiscard = "NackDiscard",
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType, // an enum to represent "durable" or "transient"
  handler: (data: T) => Promise<AckType> | AckType
): Promise<void> {
  const [channel, q] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType
  );

  await channel.consume(q.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (msg === null) return;
    try {
      const content = JSON.parse(msg.content.toString()) as T;
      const ackType = await handler(content);
      if (ackType === AckType.Ack) {
        console.log(`✅ Ack message on ${queueName}`);
        channel.ack(msg);
      } else if (ackType === AckType.NackQueue) {
        console.log(`🔁 Nack (Requeue) message on ${queueName}`);
        channel.nack(msg, false, true);
      } else if (ackType === AckType.NackDiscard) {
        console.log(`🗑️ Nack (Discard) message on ${queueName}`);
        channel.nack(msg, false, false);
      } else {
        console.warn(`⚠️ Unknown ack type, discarding message.`);
        channel.nack(msg, false, false);
      }
    } catch (err) {
      console.log("❌ Failed to process message:", err);
      channel.nack(msg, false, false);
    }
  });
}
