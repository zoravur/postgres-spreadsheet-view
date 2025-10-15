let socket: WebSocket | null = null;

export function connectWS(
  uri: string, // use ws:// for local dev
  onUpdate?: (update: any) => void
) {
  socket = new WebSocket(uri);

  socket.onopen = () => {
    console.log("✅ Connected to WebSocket");

    // Example: auto-subscribe to a base query
    sendWS({
      type: "SUBSCRIBE",
      id: "sub_main",
      query: "SELECT * FROM actor",
    });

    // Heartbeat
    setInterval(() => sendWS({ type: "PING" }), 10000);
  };

  socket.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    switch (msg.type) {
      case "SUBSCRIBED":
        console.log("🔗 Subscribed:", msg.id);
        break;
      case "UPDATE":
        if (onUpdate) onUpdate(msg);
        break;
      case "PONG":
        console.debug("pong");
        break;
      default:
        console.log("message:", msg);
    }
  };

  socket.onclose = (event) => {
    console.warn("❌ Socket closed:", event.reason || "no reason");
    setTimeout(() => connectWS(uri, onUpdate), 2000);
  };

  socket.onerror = (err) => {
    console.error("⚠️ Socket error:", err);
    socket?.close();
  };
}

export function sendWS(payload: any) {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(payload));
  } else {
    console.warn("socket not ready, dropping message:", payload);
  }
}

export function unsubscribeWS(id = "sub_main") {
  sendWS({ type: "UNSUBSCRIBE", id });
}
