import amqp, { type Channel, type Replies } from "amqplib";

export type SimpleQueueType = "durable" | "transient";

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const channel = await conn.createChannel();

  const q = await channel.assertQueue(queueName, {
    durable: queueType === "durable",
    autoDelete: queueType === "transient",
    exclusive: queueType === "transient",
    arguments: {},
  });

  await channel.bindQueue(q.queue, exchange, key);

  console.log(
    `✅ Queue "${q.queue}" bound to exchange "${exchange}" with key "${key}"`
  );

  return [channel, q];
}
