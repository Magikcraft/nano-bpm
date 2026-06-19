import Placeholder from "../components/Placeholder";

export default function Modeler() {
  return (
    <Placeholder
      title="BPMN Modeler"
      blurb="An embedded bpmn.io modeler for authoring and deploying process definitions."
      planned={[
        "bpmn-js modeler canvas + palette",
        "Deploy to the gateway (POST /v2/deployments)",
        "Open existing definitions from the cluster",
      ]}
    />
  );
}
