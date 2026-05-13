# Pluggable DataFormat Plugin - Replication & Recovery Test Plan

## Cluster Configuration
- 3 nodes: runTask-0, runTask-1, runTask-2
- Remote store enabled
- Segment replication enabled
- Plugins: analytics-engine, composite-engine, parquet-data-format, analytics-backend-lucene, analytics-backend-datafusion

## Index Configuration
- 1 primary shard, 1 replica
- Composite dataformat with parquet primary
- Segment replication type
- Fields: name (keyword), age (integer)

## Test Matrix

### Test 1: Primary-to-Replica Segment Replication
**Objective**: Verify parquet segments replicate from primary to replica node
**Steps**:
1. Create index, ingest documents, force refresh
2. Identify primary and replica node via _cat/shards
3. Check filesystem: both nodes should have parquet/ directory with files
4. Compare file counts/sizes between primary and replica
**Validation**:
- parquet/ dir exists on both primary and replica nodes
- File count matches between primary and replica
- Search returns same results when targeting primary vs replica (preference=_primary, preference=_replica)

### Test 2: Remote Store Upload
**Objective**: Verify segments are uploaded to remote store
**Steps**:
1. After indexing + refresh, check remote-store-repo directory
2. Look for segment metadata and translog data under the index UUID path
**Validation**:
- remote-store-repo/{HASH}/{INDEX_UUID}/0/segments/metadata/ has files
- remote-store-repo/{HASH}/{INDEX_UUID}/0/translog/data/ has files
- File timestamps are recent (after indexing)

### Test 3: Shard Relocation
**Objective**: Verify shard can be moved between nodes with data integrity
**Steps**:
1. Record current shard allocation
2. Use _cluster/reroute to move shard to node without shard (runTask-2)
3. Wait for relocation to complete
4. Verify parquet files exist on new node
5. Run search queries to verify data integrity
**Validation**:
- Shard successfully relocates (green health)
- parquet/ files exist on destination node
- Search returns correct results

### Test 4: Replica Promotion to Primary
**Objective**: Verify replica can be promoted to primary when primary is cancelled
**Steps**:
1. Record which node has primary
2. Cancel primary allocation using _cluster/reroute with allow_primary=true
3. Wait for replica to be promoted
4. Verify new primary serves correct data
5. Wait for new replica to be allocated
**Validation**:
- Cluster returns to green (new primary + new replica)
- Search returns correct results from new primary
- parquet/ files intact on promoted node

### Test 5: Recovery from Remote Store
**Objective**: Verify shard can recover from remote store when local data is deleted
**Steps**:
1. Stop cluster
2. Delete local shard data from one node (rm -rf the index directory)
3. Restart cluster
4. Verify shard recovers from remote store
5. Check parquet/ files are restored
**Validation**:
- Shard recovers successfully
- parquet/ directory recreated with files
- Search returns correct results
- _cat/recovery shows REMOTE_STORE as recovery type

### Test 6: Node Restart Recovery
**Objective**: Verify node recovers its shards after restart
**Steps**:
1. Note shard allocation
2. Stop one node (kill the process)
3. Wait for cluster to go yellow
4. Restart the node
5. Verify shard recovery
**Validation**:
- Node rejoins cluster
- Shards recover (existing_store or peer recovery)
- Cluster returns to green
- Search returns correct results

### Test 7: Search Validation Queries
Used after each test to verify data integrity:
- match_all query
- term query on name field
- range query on age field
- bool compound query
- PPL query (if supported)
- Count verification
