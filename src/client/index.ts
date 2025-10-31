import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind } from "../internal/pubsub/declareAndBind.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");

  const amqpConn = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(amqpConn);
  console.log("✅ Connected to RabbitMQ successfully!");

  const username: string = await clientWelcome();
  console.log(`👋 Welcome, ${username}!`);

  const queueName = `${PauseKey}.${username}`;

  const [channel, q] = await declareAndBind(
    connection,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    "transient"
  );

  process.on("SIGINT", async () => {
    console.log("\n🛑 Shutting down gracefully...");
    (await connection).close();
    console.log("🔒 Connection closed. Goodbye!");
    process.exit(0);
  });

  console.log("Press Ctrl+C to stop the server.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
