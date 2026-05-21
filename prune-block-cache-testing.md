# Block Cache Prune — Local Testing Setup

## Branch
`feature/prune-block-cache` (based on `vishwasgarg18/feature/warm-cache-spi-and-stats`)

## Prerequisites

### Java
Use the Temurin JDK 25 already cached by Gradle (has `jdk.incubator.vector`):
```
JDK25=/home/bkhishor/.gradle/caches/9.4.1/transforms/80f1cd397e4259940b78142ca0f8d70d/transformed/linux-25.0.3-x64.tar.gz
```

### Rust / native library
Install Rust (one-time):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
source ~/.cargo/env
```

Install protoc (one-time):
```bash
curl -sL https://github.com/protocolbuffers/protobuf/releases/download/v25.3/protoc-25.3-linux-x86_64.zip -o /tmp/protoc.zip
unzip -q /tmp/protoc.zip -d /tmp/protoc
```

Build the native library:
```bash
source ~/.cargo/env
export PROTOC=/tmp/protoc/bin/protoc
cd ~/ws/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build --release
# Output: sandbox/libs/dataformat-native/rust/target/release/libopensearch_native.so
```

### Build the Foyer plugin
```bash
JDK25=/home/bkhishor/.gradle/caches/9.4.1/transforms/80f1cd397e4259940b78142ca0f8d70d/transformed/linux-25.0.3-x64.tar.gz
cd ~/ws/OpenSearch
JAVA_HOME=$JDK25 ./gradlew :sandbox:plugins:block-cache-foyer:bundlePlugin --no-daemon -Dsandbox.enabled=true
# Output: sandbox/plugins/block-cache-foyer/build/distributions/block-cache-foyer-3.7.0-SNAPSHOT.zip
```

Install into the distribution:
```bash
DIST=~/ws/OpenSearch/distribution/archives/linux-tar/build/install/opensearch-3.7.0-SNAPSHOT
PLUGIN_ZIP=~/ws/OpenSearch/sandbox/plugins/block-cache-foyer/build/distributions/block-cache-foyer-3.7.0-SNAPSHOT.zip
$DIST/bin/opensearch-plugin install --batch "file://$PLUGIN_ZIP"
```

---

## Starting the cluster

### Node 1 — cluster manager (port 9200/9300)
```bash
JDK25=/home/bkhishor/.gradle/caches/9.4.1/transforms/80f1cd397e4259940b78142ca0f8d70d/transformed/linux-25.0.3-x64.tar.gz
cd ~/ws/OpenSearch
JAVA_HOME=$JDK25 nohup ./gradlew run --no-daemon -Dtests.heap.size=2g > /tmp/opensearch-run.log 2>&1 &
# Wait for: curl http://localhost:9200 → 200
```

### Node 2 — warm node (port 9202/9303)

Create config:
```bash
mkdir -p /tmp/opensearch-warm-node/config /tmp/opensearch-warm-node/data /tmp/opensearch-warm-node/logs

DIST=~/ws/OpenSearch/distribution/archives/linux-tar/build/install/opensearch-3.7.0-SNAPSHOT
cp $DIST/config/jvm.options /tmp/opensearch-warm-node/config/
cp $DIST/config/log4j2.properties /tmp/opensearch-warm-node/config/

cat > /tmp/opensearch-warm-node/config/opensearch.yml << 'EOF'
cluster.name: runTask
node.name: warm-node-0
node.roles: [warm]
http.port: 9202
transport.port: 9303
path.data: /tmp/opensearch-warm-node/data
path.logs: /tmp/opensearch-warm-node/logs
discovery.seed_hosts: ["127.0.0.1:9300"]
cluster.initial_cluster_manager_nodes: []
EOF
```

Start:
```bash
DIST=~/ws/OpenSearch/distribution/archives/linux-tar/build/install/opensearch-3.7.0-SNAPSHOT
JDK25=/home/bkhishor/.gradle/caches/9.4.1/transforms/80f1cd397e4259940b78142ca0f8d70d/transformed/linux-25.0.3-x64.tar.gz
NATIVE_LIB=/home/bkhishor/ws/OpenSearch/sandbox/libs/dataformat-native/rust/target/release

JAVA_HOME=$JDK25 \
OPENSEARCH_PATH_CONF=/tmp/opensearch-warm-node/config \
OPENSEARCH_JAVA_OPTS="-Djava.library.path=$NATIVE_LIB --enable-native-access=ALL-UNNAMED" \
nohup $DIST/bin/opensearch \
  -E path.data=/tmp/opensearch-warm-node/data \
  -E path.logs=/tmp/opensearch-warm-node/logs \
  > /tmp/opensearch-warm.log 2>&1 &
```

Verify both nodes are up:
```bash
curl -s http://localhost:9200/_cat/nodes?v
# Expected: runTask-0 (dimr) + warm-node-0 (w)
```

---

## Test commands

### Happy path — prune executes on warm node
```bash
curl -s -X POST "http://localhost:9200/_blockcache/prune?cache=disk&pretty"
```
Expected response:
```json
{
  "acknowledged": true,
  "summary": { "total_nodes_targeted": 1, "successful_nodes": 1, "failed_nodes": 0 },
  "nodes": {
    "<node-id>": { "name": "warm-node-0", "cleared": true }
  }
}
```

Confirm in warm node logs (`/tmp/opensearch-warm.log`):
```
ffm: foyer_clear_cache completed
Foyer block cache cleared
```

### Target a specific node
```bash
NODE_ID=$(curl -s http://localhost:9200/_nodes | python3 -c "
import sys,json; nodes=json.load(sys.stdin)['nodes']
print(next(k for k,v in nodes.items() if 'warm' in v.get('roles',[])))
")
curl -s -X POST "http://localhost:9200/_blockcache/prune?cache=disk&nodes=$NODE_ID&pretty"
```

### Error cases
```bash
# Missing cache param → 400
curl -s -X POST "http://localhost:9200/_blockcache/prune?pretty"

# Unknown cache name → 400
curl -s -X POST "http://localhost:9200/_blockcache/prune?cache=foyer&pretty"
curl -s -X POST "http://localhost:9200/_blockcache/prune?cache=unknown&pretty"

# Non-warm node ID → 400
curl -s -X POST "http://localhost:9200/_blockcache/prune?cache=disk&nodes=<data-node-id>&pretty"
```

---

## Notes
- `?cache=disk` is the correct param — `disk` is the role-based name from `BlockCacheConstants.DISK_CACHE`, not the library name.
- On a single-node cluster (no warm nodes), the happy path returns `total_nodes_targeted: 0` — that's expected.
- The warm node needs `OPENSEARCH_JAVA_OPTS` with `java.library.path` pointing to the Rust release dir, otherwise Foyer fails to load the native library.
- Logs: cluster manager at `/tmp/opensearch-run.log`, warm node at `/tmp/opensearch-warm.log`.
