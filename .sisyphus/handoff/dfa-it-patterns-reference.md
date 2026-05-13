# DFA Integration Test Patterns Reference

## Section A — Existing DFA IT Inheritance Chain

**Base class**: `RemoteStoreBaseIntegTestCase` (extends `OpenSearchIntegTestCase`)
- File: `test/framework/src/main/java/org/opensearch/remotestore/RemoteStoreBaseIntegTestCase.java`
- Provides: remote-store repo setup (segmentRepoPath, translogRepoPath), `remoteStoreIndexSettings()`, `prepareCluster()`, `getIndexShard(String dataNode, String indexName)`:331

**All 3 existing DFA ITs extend `RemoteStoreBaseIntegTestCase` directly** — there is NO intermediate DFA base class.

**Plugin registration** (identical in all 3 ITs):
- `DataFormatAwareReplicationIT.java:44` / `DataFormatAwareRemoteStoreRecoveryIT.java:48` / `DataFormatAwareUploadIT.java:42`
```java
@Override
protected Collection<Class<? extends Plugin>> nodePlugins() {
    return Stream.concat(
        super.nodePlugins().stream(),
        Stream.of(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class)
    ).collect(Collectors.toList());
}
```

**Feature flag** (identical in all 3 ITs):
- `DataFormatAwareReplicationIT.java:52` / `DataFormatAwareRemoteStoreRecoveryIT.java:56` / `DataFormatAwareUploadIT.java:50`
```java
@Override
protected Settings nodeSettings(int nodeOrdinal) {
    return Settings.builder()
        .put(super.nodeSettings(nodeOrdinal))
        .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
        .build();
}
```

**Cluster scope annotation** (all 3 ITs):
```java
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
```

**No `@SuppressForbidden` or `@LockFeatureFlag` annotations used.**

## Section B — How to Create a DFA-Enabled Index in an IT

Minimum required index settings (from `DataFormatAwareReplicationIT.java:60`):
```java
protected Settings dfaIndexSettings(int replicaCount) {
    return Settings.builder()
        .put(remoteStoreIndexSettings(replicaCount, 1))  // from base: 1 shard, segment rep, remote store
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put("index.pluggable.dataformat.enabled", true)
        .put("index.pluggable.dataformat", "composite")
        .put("index.composite.primary_data_format", "parquet")
        .putList("index.composite.secondary_data_formats", List.of())
        .build();
}
```

**Conventions**:
- Shard count: always 1
- Replica count: 0 (upload tests), 1 (replication tests), 2 (fan-out tests)
- No explicit refresh-interval override (inherits 300s from `RemoteStoreBaseIntegTestCase.defaultIndexSettings()`)
- No translog overrides
- RefreshPolicy.NONE on individual docs; explicit flush/refresh at test boundaries

**Cluster startup pattern** (ReplicationIT:58):
```java
internalCluster().startClusterManagerOnlyNode();
internalCluster().startDataOnlyNodes(dataNodes);
client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(replicaCount)).get();
ensureYellowAndNoInitializingShards(INDEX_NAME);
ensureGreen(INDEX_NAME);
```

**Upload IT uses `prepareCluster(1, 1, Settings.EMPTY)` then `createDfaIndex(0)`.**

## Section C — Node Lifecycle Idioms

**Graceful stop primary** (SegmentReplicationIT:400):
```java
internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));
ensureYellowAndNoInitializingShards(INDEX_NAME);
```

**Hard kill primary (crash simulation)** — same API, no graceful shutdown:
```java
internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
// No ensureYellow — immediately check promotion
```

**Wait for replica promotion** (SegmentReplicationIT:404):
```java
ShardRouting replicaShardRouting = getShardRoutingForNodeName(replicaNode);
assertTrue(replicaShardRouting.primary());
```

**Restart a node preserving data dir** (DFA RecoveryIT:233, SegmentReplicationIT:441):
```java
internalCluster().restartNode(nodeName);
ensureGreen(INDEX_NAME);
```

**Force file-based peer recovery (full remote-store restore)** (DFA RecoveryIT:148-152):
```java
internalCluster().stopRandomNode(s -> dataNode.equals(s));
ensureRed(INDEX_NAME);
internalCluster().startDataOnlyNode();
client().admin().indices().prepareClose(INDEX_NAME).get();
client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
ensureGreen(INDEX_NAME);
```

## Section D — BackgroundIndexer Patterns

**Construction** (SegmentReplicationIT:1131):
```java
try (BackgroundIndexer indexer = new BackgroundIndexer(INDEX_NAME, "_doc", client(), -1,
        RandomizedTest.scaledRandomIntBetween(2, 5), false, random())) {
    indexer.start(initialDocCount);
    waitForDocs(initialDocCount, indexer);
    // ... test logic ...
}
```

**Await + capture ack'd count** (RecoveryWhileUnderLoadIT:135):
```java
waitForDocs(waitFor, indexer);
indexer.assertNoFailures();
indexer.continueIndexing(extraDocs);
// ... after test ...
indexer.stopAndAwaitStopped();
iterateAssertCount(numberOfShards, 10, indexer.getIds());
```

**Key APIs**: `start(int numDocs)`, `continueIndexing(int numDocs)`, `pauseIndexing()`, `stopAndAwaitStopped()`, `totalIndexedDocs()`, `assertNoFailures()`, `getIds()` → Set<String>, `setIgnoreIndexingFailures(boolean)`, `setRequestTimeout(TimeValue)`.

**Gotchas**: 
- `numOfDocs=-1` means unlimited (must call `pauseIndexing()` or `stop()`)
- `autoStart=false` requires explicit `start(n)` call
- `waitForDocs()` is a static helper on `OpenSearchIntegTestCase`

## Section E — Assertion Helpers to Copy/Adapt

**DFA-specific** (from `DataFormatAwareITUtils.java`):
- `assertCatalogMatchesLocalAndRemote(IndexShard)` — catalog ⊆ local ∩ remote (:57)
- `assertCatalogMatchesUploadedBlobs(IndexShard, Client, Settings, Path)` — upload map + blob on disk (:76)
- `assertPrimaryUploadMapOnReplicaDisk(IndexShard primary, IndexShard replica)` — cross-shard file presence (:99)
- `catalogFilesExcludingSegments(IndexShard)` → Set<String> (:127)
- `catalogFiles(IndexShard)` → Set<String> (:121)

**Segrep-specific** (from `SegmentReplicationBaseIT.java`):
- `waitForSearchableDocs(long docCount, String... nodes)` (:126)
- `verifyStoreContent()` — primary/replica segment metadata equality (:138)
- `assertReplicaCheckpointUpdated(IndexShard primaryShard)` (:233)
- `assertEqualSegmentInfosVersion(List<String> replicaNodes, IndexShard primaryShard)` (in base)

**DFA convergence assertion pattern** (ReplicationIT:107):
```java
assertBusy(() -> {
    IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
    IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);
    RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
    RemoteSegmentMetadata rMeta = replica.getRemoteDirectory().readLatestMetadataFile();
    assertEquals(pMeta.getReplicationCheckpoint().getSegmentInfosVersion(),
                 rMeta.getReplicationCheckpoint().getSegmentInfosVersion());
    DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
    DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);
    DataFormatAwareITUtils.assertPrimaryUploadMapOnReplicaDisk(primary, replica);
}, 60, TimeUnit.SECONDS);
```

## Section F — Transport-Disruption Recipes

**addSendBehavior to block file chunks** (SegmentReplicationIT:764):
```java
MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, primaryNode));
primaryTransportService.addSendBehavior(
    internalCluster().getInstance(TransportService.class, replicaNode),
    (connection, requestId, action, request, options) -> {
        if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
            blockLatch.await();  // block until released
        }
        connection.sendRequest(requestId, action, request, options);
    });
```

**addFailToSendNoConnectRule** (MockTransportService:362):
```java
mockTransportService.addFailToSendNoConnectRule(targetTransportService);
// or with specific actions:
mockTransportService.addFailToSendNoConnectRule(targetTransportService, "action1", "action2");
```

**addUnresponsiveRule** (MockTransportService:420):
```java
mockTransportService.addUnresponsiveRule(targetTransportService);
// or with duration:
mockTransportService.addUnresponsiveRule(targetTransportService, TimeValue.timeValueSeconds(30));
```

**addRequestHandlingBehavior** (SegmentReplicationPrimaryPromotionIT:136):
```java
replicaTransportService.addRequestHandlingBehavior(
    PublishCheckpointAction.ACTION_NAME + TransportReplicationAction.REPLICA_ACTION_SUFFIX,
    (handler, request, channel, task) -> {
        throw new RemoteTransportException("mock", new OpenSearchRejectedExecutionException());
    });
```

**blockReplication helper** (SegmentReplicationBaseIT:203) — blocks GET_SEGMENT_FILES action on replicas, returns Releasable.

## Section G — Remote-Store Setup Requirements

**Automatic via `RemoteStoreBaseIntegTestCase`**:
- `segmentRepoPath` and `translogRepoPath` auto-created in `nodeSettings()`
- Repository type: `MockFsRepositoryPlugin` or `MockFsMetadataSupportedRepositoryPlugin` (random)
- No manual repository registration needed
- `@After` teardown asserts repos exist on all nodes + cleans up

**Cluster settings**: none needed beyond what `RemoteStoreBaseIntegTestCase.nodeSettings()` provides.

**Feature flags for DFA**: `FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG = true` in `nodeSettings()`.

**Which ITs use remote store today**:
- All 3 DFA ITs (extend `RemoteStoreBaseIntegTestCase`)
- `SegmentReplicationBaseIT` does NOT use remote store (extends `OpenSearchIntegTestCase` directly)

## Section H — Concrete File Paths for New Tests

**Test package path**: `sandbox/plugins/composite-engine/src/internalClusterTest/java/org/opensearch/composite/`

**Base class to extend**: `RemoteStoreBaseIntegTestCase` (same as existing DFA ITs)
- Path: `test/framework/src/main/java/org/opensearch/remotestore/RemoteStoreBaseIntegTestCase.java`

**Utility class**: `DataFormatAwareITUtils` in same package (already exists, reuse directly)

**Naming conventions**:
- `DataFormatAware*IT.java` (all existing tests follow this)
- Index name constant: `"dfa-<purpose>-test-idx"` (e.g., `"dfa-replication-test-idx"`)

**Required boilerplate for every new DFA IT** (copy from any existing):
1. `@OpenSearchIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)`
2. Override `nodePlugins()` → add 4 plugins
3. Override `nodeSettings()` → add feature flag
4. Define `dfaIndexSettings(int replicaCount)` method
5. Define `indexDocs(int count)` with RefreshPolicy.NONE
6. Define `primaryNodeName()` / `replicaNodeNames()` helpers

**No intermediate base class exists** — if creating many new ITs, consider extracting a `DataFormatAwareBaseIT` with the shared boilerplate above.
