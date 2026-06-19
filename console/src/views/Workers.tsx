import Placeholder from "../components/Placeholder";

export default function Workers() {
  return (
    <Placeholder
      title="Embedded Workers"
      blurb="Author and run job workers directly in the browser, over the gateway's command stream."
      planned={[
        "Monaco editor for worker handler code",
        "Sandboxed Web Worker runtime executing user JS",
        "Command-stream WebSocket client (browser port of clients/node-stream)",
        "Live job activation / completion log",
      ]}
    />
  );
}
