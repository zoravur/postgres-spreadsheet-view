let socket: WebSocket;

export function connectWS(
  uri: string = `wss://${window.location.host}/api/ws`,
  onmessage: (this: WebSocket, ev: MessageEvent) => any
) {
  socket = new WebSocket(uri);

  socket.onopen = () => {
    console.log("✅ Connected to WebSocket");
  };

  socket.onmessage = onmessage;

  socket.onclose = (event) => {
    console.warn("❌ Socket closed:", event.reason);
    // optional: auto-reconnect
    setTimeout(connectWS, 2000);
  };

  socket.onerror = (err) => {
    console.error("⚠️ Socket error:", err);
    socket.close();
  };
}
