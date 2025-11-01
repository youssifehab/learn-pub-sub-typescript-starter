import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { declareAndBind } from "../internal/pubsub/declareAndBind.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import {
  GameState,
  type PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribeJSON.js";

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

  const gameState = new GameState(username);
  console.log(`🧠 Game state initialized for ${username}`);

  await subscribeJSON(
    connection,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    "transient",
    handlerPause(gameState)
  );

  console.log(`🎮 Listening for pause/resume messages on ${queueName}`);

  while (true) {
    const words = await getInput("> ");

    if (words.length === 0) continue;

    if (words[0] === "spawn") {
      try {
        commandSpawn(gameState, words);
      } catch (err) {
        if (err instanceof Error) {
          console.log(err.message);
        } else {
          console.log(err);
        }
      }
    } else if (words[0] === "move") {
      try {
        const move = commandMove(gameState, words);
        console.log(move);
      } catch (err) {
        if (err instanceof Error) {
          console.log(err.message);
        } else {
          console.log(err);
        }
      }
    } else if (words[0] === "status") {
      await commandStatus(gameState);
    } else if (words[0] === "help") {
      printClientHelp();
    } else if (words[0] === "spam") {
      console.log("Spamming not allowed yet!");
    } else if (words[0] === "quit") {
      printQuit();
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

function handlerPause(gs: GameState): (ps: PlayingState) => void {
  return (ps: PlayingState) => {
    if (ps.isPaused) {
      gs.pauseGame();
    } else {
      gs.resumeGame();
    }
  };
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
