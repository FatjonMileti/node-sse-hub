/**
 * Topics example for `sse-kit`: subscribe connections to rooms and
 * broadcast per-room updates.
 *
 * Run with:  npx tsx examples/topics.ts
 * (Install express to try this file.)
 */
import express from "express";
import { SSEServer } from "../src/index.js";

const sse = new SSEServer({ heartbeatInterval: 30_000 });

const app = express();
app.use(express.json());

// Clients subscribe via query param, e.g. /events?topics=invoices,alerts
app.get("/events", (req, res) => {
  const connection = sse.connect(req, res);
  const raw = req.query["topics"];
  const values = Array.isArray(raw) ? raw : [raw];
  const topics = values
    .filter((t): t is string => typeof t === "string")
    .flatMap((t) => t.split(","))
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
  for (const topic of topics) {
    connection.subscribe(topic);
  }
  console.log(`Client ${connection.id} subscribed to:`, topics);
});

app.post("/invoices/:id", (req, res) => {
  const count = sse.to("invoices").broadcast({
    event: "invoice-updated",
    data: { id: req.params["id"] },
  });
  res.json({ delivered: count });
});

app.listen(3000, () => {
  console.log("Listening on http://localhost:3000");
});
