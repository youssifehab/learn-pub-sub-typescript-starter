import amqp, { type ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publishJSON.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";

async function main() {
  console.log("Starting Peril server...");

  printServerHelp();

  const apqpConn = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(apqpConn);
  console.log("✅ Connected to RabbitMQ successfully!");

  const channel: ConfirmChannel = await connection.createConfirmChannel();
  console.log("📡 Confirm channel created!");

  while (true) {
    const words = await getInput("> ");
    if (words.length === 0) continue;

    if (words[0] === "pause") {
      console.log("⏸ Sending pause message...");
      const message: PlayingState = { isPaused: true };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, message);
      console.log(
        "📨 Published pause message to exchange:",
        ExchangePerilDirect
      );
    } else if (words[0] === "resume") {
      console.log("▶️ Sending resume message...");
      const message: PlayingState = { isPaused: false };
      await publishJSON(channel, ExchangePerilDirect, PauseKey, message);
      console.log("📨 Resume message published!", ExchangePerilDirect);
    } else if (words[0] === "quit") {
      console.log("👋 Exiting server...");
      break;
    } else {
      console.log(`❓ Unknown command: ${words[0]}`);
    }
  }

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
