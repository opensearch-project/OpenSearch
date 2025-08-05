# Chaos Testing

The Arrow Flight RPC plugin includes chaos testing capabilities to simulate network failures and test resilience.

## Enabling Chaos Testing

Chaos testing is disabled by default. To enable it, modify the `build.gradle` file:

### 1. Add Chaos Agent to internalClusterTest

Add this to the `internalClusterTest` task:

```gradle

internalClusterTest {
  // Enable chaos testing via bytecode injection
  doFirst {
    def agentJar = createChaosAgent()
    jvmArgs "-javaagent:${agentJar}"
  }
}
```

### 2. Add Chaos Agent Creation Task

Add this task to create the chaos agent JAR:

```gradle
// Task to create chaos agent JAR
def createChaosAgent() {
  def agentJar = file("${buildDir}/chaos-agent.jar")

  if (!agentJar.exists()) {
    def manifestFile = file("${buildDir}/MANIFEST.MF")
    manifestFile.text = '''Manifest-Version: 1.0
Premain-Class: org.opensearch.arrow.flight.chaos.ChaosAgent
Agent-Class: org.opensearch.arrow.flight.chaos.ChaosAgent
Can-Redefine-Classes: true
Can-Retransform-Classes: true
'''
    ant.jar(destfile: agentJar, manifest: manifestFile) {
      fileset(dir: sourceSets.internalClusterTest.output.classesDirs.first(), includes: 'org/opensearch/arrow/flight/chaos/ChaosAgent*.class')
    }
  }

  return agentJar.absolutePath
}
```

## Running Chaos Tests

Once enabled, run the chaos tests with:

```bash
./gradlew :plugins:arrow-flight-rpc:internalClusterTest --tests="*Chaos*"
```

## What Chaos Testing Does

The chaos testing framework:
- Injects bytecode to simulate network failures
- Tests client-side resilience to connection drops
- Validates proper error handling and recovery
- Ensures graceful degradation under adverse conditions
