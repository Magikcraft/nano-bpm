# nano-bernd (JVM)

Embedded Nano BPMN engine for the JVM. Codename **Bernd** — see [ADR 0005](../../docs/adr/0005-embedded-u-nano.md).

Runs `nano_engine.wasm` (ABI v2) through [Chicory](https://chicory.dev), a pure-Java WebAssembly runtime — no JNI, no native binaries, no `graal-sdk` needed at runtime. Works on standard JVM and compiles cleanly with GraalVM Native Image.

## Signature

```
      _   _                       ____                     _
     | \ | | __ _ _ __   ___     | __ )  ___ _ __ _ __  __| |
     |  \| |/ _` | '_ \ / _ \    |  _ \ / _ \ '__| '_ \/ _` |
     | |\  | (_| | | | | (_) |   | |_) |  __/ |  | | | | (_| |
     |_| \_|\__,_|_| |_|\___/    |____/ \___|_|  |_| |_|\__,_|

     Named for Bernd Ruecker, whose talks on decoupled workers,
     Sagas and the compensation pattern are the intellectual source
     of the embedded-engine design realised here.
     Artists sign their work. See ADR 0015.
```

## Usage

```java
try (EmbeddedEngine engine = EmbeddedEngine.create()) {
  engine.deploy(Files.readString(Path.of("order.bpmn")));
  var instance = engine.createInstance("order-fulfilment");

  for (ActivatedJob job : engine.activateJobs("charge-card", "worker-1", 10, 30_000)) {
    // ... call your service ...
    engine.completeJob(job.key());
  }
}
```

`EmbeddedEngine.CODENAME` = `"Bernd"`.
