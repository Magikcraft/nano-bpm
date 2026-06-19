import Placeholder from "../components/Placeholder";

export default function Explorer() {
  return (
    <Placeholder
      title="Process Instance Explorer"
      blurb="An Operate-style live view of running and completed process instances."
      planned={[
        "List/search instances from the read model",
        "Instance detail with token state overlaid on the BPMN diagram",
        "Live updates via SSE from the gateway",
        "Incident inspection and resolution",
      ]}
    />
  );
}
