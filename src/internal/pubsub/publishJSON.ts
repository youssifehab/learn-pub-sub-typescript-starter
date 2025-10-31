import amqp, { type ConfirmChannel } from "amqplib";

export function publishJSON<T>(
  ch: amqp.ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T
): Promise<void> {
  const json = JSON.stringify(value);
  const buffer = Buffer.from(json, "utf-8");

  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      buffer,
      {
        contentType: "application/json",
      },
      (err, ok) => {
        if (err) reject(err);
        else resolve();
      }
    );
  });
}
