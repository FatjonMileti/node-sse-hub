import { SSEServer } from "node-sse-hub";
import type {
  BroadcastOptions,
  SSEEvent,
  SSEServerOptions,
  SSEServerResolvedOptions,
} from "node-sse-hub";

const options: SSEServerOptions = {
  heartbeatInterval: 15_000,
  history: { enabled: true },
};

const broadcastOptions: BroadcastOptions = {
  exceptConnectionIds: ["sender-connection-id", "unknown-id"],
};

export function create(): SSEServerResolvedOptions {
  const sse = new SSEServer(options);
  const event: SSEEvent<{ orderId: string }> = {
    event: "order-updated",
    data: { orderId: "7" },
  };
  sse.broadcast(event, broadcastOptions);
  sse.send(event);
  return sse.getOptions();
}
