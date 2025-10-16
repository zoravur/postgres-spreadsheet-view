let socket: WebSocket | null = null;
let heartbeat: ReturnType<typeof setInterval> | null = null;

export interface WSMessage {
  type: string;
  data?: any;
}

export function connectWS(
  uri: string, // ws://localhost:8080/api/ws  (or wss:// in prod)
  onUpdate?: (payload: any) => void
) {
  socket = new WebSocket(uri);

  socket.onopen = () => {
    console.log("✅ Connected to WebSocket");

    // Example: auto-subscribe on connect
    // subscribeWS("SELECT * FROM actor");

    // Heartbeat (keep connection alive)
    // heartbeat = setInterval(() => {
    //   sendWS({ type: "ping" });
    // }, 10000);
  };

  socket.onmessage = (ev) => {
    let msg: WSMessage;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      console.warn("Invalid WS JSON:", ev.data);
      return;
    }

    const type = msg.type?.toLowerCase?.();

    switch (type) {
      case "subscribed":
        console.log("🔗 Subscribed:", msg.data?.id);
        console.debug("tables:", msg.data?.tables);
        break;

      case "unsubscribed":
        console.log("🔌 Unsubscribed");
        break;

      case "update":
        if (onUpdate) onUpdate(msg.data);
        else console.log("Update:", msg.data);
        break;

      case "error":
        console.error("WS Error:", msg.data?.error || msg.data);
        break;

      case "pong":
        console.debug("PONG");
        break;

      default:
        console.log("WS message:", msg);
    }
  };

  socket.onclose = (event) => {
    console.warn("❌ Socket closed:", event.reason || "no reason");
    if (heartbeat) clearInterval(heartbeat);
    // Auto-reconnect
    setTimeout(() => connectWS(uri, onUpdate), 2000);
  };

  socket.onerror = (err) => {
    console.error("⚠️ Socket error:", err);
    socket?.close();
  };
}

// --- protocol helpers ---

export function sendWS(payload: Record<string, any>) {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  } else {
    console.warn("⏳ Socket not ready, dropping message:", payload);
  }
}

export function subscribeWS(sql: string) {
  sendWS({
    type: "subscribe",
    sql,
  });
}

export function unsubscribeWS() {
  sendWS({ type: "unsubscribe" });
}
