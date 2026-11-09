/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkItemRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.action.bulk.TransportShardBulkAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.instanceOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class ShardIndexingPressureSettingsIT extends OpenSearchIntegTestCase {

    public static final String INDEX_NAME = "test_index";

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    public static final Settings settings = Settings.builder()
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
        .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
        .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(unboundedWriteQueue).put(settings).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testNodeAttributeSetForShardIndexingPressure() throws Exception {
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        ensureGreen(INDEX_NAME);

        for (DiscoveryNode nodes : client().admin().cluster().prepareState().get().getState().nodes()) {
            assertEquals("true", nodes.getAttributes().get("shard_indexing_pressure_enabled"));
        }
    }

    public void testShardIndexingPressureFeatureEnabledDisabledSetting() {
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        ensureGreen(INDEX_NAME);
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);
        IndexingPressure primaryNodeLimits = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure();
        IndexingPressure replicaNodeLimits = internalCluster().getInstance(IndexingPressureService.class, replicaName)
            .getShardIndexingPressure();
        IndexingPressure coordinatingNodeLimits = internalCluster().getInstance(IndexingPressureService.class, coordinatingOnlyNode)
            .getShardIndexingPressure();

        // Assert no tracking at shard level for indexing pressure since default setting is disabled
        ShardIndexingPressure shardIndexingPressure = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure();
        assertFalse(shardIndexingPressure.isShardIndexingPressureEnabled());
        assertNull(shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId));
        assertEquals(bulkRequest.ramBytesUsed(), coordinatingNodeLimits.totalCoordinatingBytes.get());
        assertEquals(bulkShardRequestSize, primaryNodeLimits.totalPrimaryBytes.get());
        assertEquals(bulkShardRequestSize, replicaNodeLimits.totalReplicaBytes.get());

        // Enable the setting for shard indexing pressure as true
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Send a second request
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
            IndexingPressureService.class,
            coordinatingOnlyNode
        ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

        assertTrue(shardIndexingPressure.isShardIndexingPressureEnabled());
        assertEquals(
            bulkShardRequestSize,
            coordinatingShardTracker.getCommonOperationTracker().getTotalCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.getCoordinatingOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.getCommonOperationTracker().getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getTotalBytes());

        assertEquals(bulkRequest.ramBytesUsed() + bulkShardRequestSize, coordinatingNodeLimits.totalCoordinatingBytes.get());
        assertEquals(2 * bulkShardRequestSize, primaryNodeLimits.totalPrimaryBytes.get());
        assertEquals(2 * bulkShardRequestSize, replicaNodeLimits.totalReplicaBytes.get());

        // Disable the setting again for shard indexing pressure as true
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Send a third request which should not be tracked since feature is disabled again
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        assertFalse(shardIndexingPressure.isShardIndexingPressureEnabled());
        assertEquals(
            bulkShardRequestSize,
            coordinatingShardTracker.getCommonOperationTracker().getTotalCombinedCoordinatingAndPrimaryBytes()
        );
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.getCoordinatingOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.getCommonOperationTracker().getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getTotalBytes());

        assertEquals(2 * bulkRequest.ramBytesUsed() + bulkShardRequestSize, coordinatingNodeLimits.totalCoordinatingBytes.get());
        assertEquals(3 * bulkShardRequestSize, primaryNodeLimits.totalPrimaryBytes.get());
        assertEquals(3 * bulkShardRequestSize, replicaNodeLimits.totalReplicaBytes.get());
    }

    public void testShardIndexingPressureNodeLimitUpdateSetting() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        // Set the Node limit threshold above the request-size; for no rejection
        restartCluster(
            Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B")
                .build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        assertNoFailures(successFuture.actionGet());

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
            IndexingPressureService.class,
            coordinatingOnlyNode
        ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

        // Tracking done with no rejections
        assertEquals(0, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
        assertEquals(bulkShardRequestSize, coordinatingShardTracker.getCoordinatingOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, primaryShardTracker.getPrimaryOperationTracker().getStatsTracker().getTotalBytes());
        assertEquals(bulkShardRequestSize, replicaShardTracker.getReplicaOperationTracker().getStatsTracker().getTotalBytes());

        // Update the indexing byte setting to a lower value
        restartCluster(
            Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 0.5) + "B")
                .build()
        );

        // Any node receiving the request will end up rejecting request due to node level limit breached
        final BulkResponse rejectedResponse;
        if (randomBoolean()) {
            rejectedResponse = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
        } else if (randomBoolean()) {
            rejectedResponse = client(primaryName).bulk(bulkRequest).actionGet();
        } else {
            rejectedResponse = client(replicaName).bulk(bulkRequest).actionGet();
        }
        assertRejectedBulkResponse(bulkRequest, rejectedResponse);
    }

    public void testShardIndexingPressureEnforcedEnabledDisabledSetting() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 3.8) + "B")
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "10ms")
            .build();
        restartCluster(settings);

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
            IndexingPressureService.class,
            coordinatingOnlyNode
        ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

        // Send first request which gets successful to set the last successful time-stamp
        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        assertNoFailures(successFuture.actionGet());

        // Send couple of more requests which remains outstanding
        Releasable primaryReleasable = blockPrimary(primaryName);
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        ActionFuture<BulkResponse> secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        waitForLastSuccessfulRequestTimeout(coordinatingShardTracker);

        // This request breaches the threshold and hence will be rejected
        assertRejectedBulkResponse(bulkRequest, client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());
        assertEquals(1, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
        assertEquals(
            1,
            coordinatingShardTracker.getCoordinatingOperationTracker()
                .getRejectionTracker()
                .getLastSuccessfulRequestLimitsBreachedRejections()
        );

        // Complete the outstanding requests
        primaryReleasable.close();
        assertNoFailures(successFuture.actionGet());
        assertNoFailures(secondSuccessFuture.actionGet());

        // Disable the enforced mode by setting it to false
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), false)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Repeat the previous set of request patterns; but no actual rejection this time
        // First successful request
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        assertNoFailures(successFuture.actionGet());

        // Send couple of requests which remains outstanding
        primaryReleasable = blockPrimary(primaryName);
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);

        waitForLastSuccessfulRequestTimeout(coordinatingShardTracker);

        // Send the third request which will not get rejected; unlike previous time
        ActionFuture<BulkResponse> thirdSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        assertBusy(
            () -> assertEquals(
                3,
                coordinatingShardTracker.getCoordinatingOperationTracker().getPerformanceTracker().getTotalOutstandingRequests()
            )
        );

        // No new actual rejection
        assertEquals(1, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
        // Shadow mode rejection count breakup still updated
        assertEquals(
            2,
            coordinatingShardTracker.getCoordinatingOperationTracker()
                .getRejectionTracker()
                .getLastSuccessfulRequestLimitsBreachedRejections()
        );

        primaryReleasable.close();
        assertNoFailures(successFuture.actionGet());
        assertNoFailures(secondSuccessFuture.actionGet());
        assertNoFailures(thirdSuccessFuture.actionGet());

        // Enable the enforced mode again by setting it to true
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Repeat the previous set of requests; to get an actual rejection again this time
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        assertNoFailures(successFuture.actionGet());

        // Send couple of requests which remains outstanding
        primaryReleasable = blockPrimary(primaryName);
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        waitForLastSuccessfulRequestTimeout(coordinatingShardTracker);

        // This request breaches the threshold and hence will be rejected
        assertRejectedBulkResponse(bulkRequest, client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());

        // new rejection added to the actual rejection count
        assertEquals(2, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
        assertEquals(
            3,
            coordinatingShardTracker.getCoordinatingOperationTracker()
                .getRejectionTracker()
                .getLastSuccessfulRequestLimitsBreachedRejections()
        );

        primaryReleasable.close();
        assertNoFailures(successFuture.actionGet());
        assertNoFailures(secondSuccessFuture.actionGet());
    }

    public void testShardIndexingPressureEnforcedEnabledNoOpIfFeatureDisabled() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), false)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .build();
        restartCluster(settings);

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        // Send first request
        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        // Verify No actual tracking being done
        ShardIndexingPressure shardIndexingPressure = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure();
        assertFalse(shardIndexingPressure.isShardIndexingPressureEnabled());
        assertNull(shardIndexingPressure.coldStats().getIndexingPressureShardStats(shardId));
    }

    public void testShardIndexingPressureVerifyShardMinLimitSettingUpdate() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        // Set the Node limit threshold above the request-size; for no rejection
        long initialNodeLimit = 1000000;
        restartCluster(
            Settings.builder()
                .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
                .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) initialNodeLimit + "B")
                .build()
        );

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
            IndexingPressureService.class,
            coordinatingOnlyNode
        ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);

        // Verify initial shard limits
        assertEquals(1000, coordinatingShardTracker.getPrimaryAndCoordinatingLimits());
        assertEquals(1000, primaryShardTracker.getPrimaryAndCoordinatingLimits());
        assertEquals(1500, replicaShardTracker.getReplicaLimits());

        // New Increment factor
        double incrementFactor = 0.01d;
        // Update the setting for initial shard limit
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureSettings.SHARD_MIN_LIMIT.getKey(), incrementFactor)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Create a new index to send new request
        assertAcked(
            prepareCreate(
                INDEX_NAME + "new",
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME + "new");

        primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME + "new");
        primaryName = primaryReplicaNodeNames.v1();
        replicaName = primaryReplicaNodeNames.v2();
        coordinatingOnlyNode = getCoordinatingOnlyNode();

        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        index = indexService.getIndexSettings().getIndex();
        shardId = new ShardId(index, 0);

        primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        replicaShardTracker = internalCluster().getInstance(IndexingPressureService.class, replicaName)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);
        coordinatingShardTracker = internalCluster().getInstance(IndexingPressureService.class, coordinatingOnlyNode)
            .getShardIndexingPressure()
            .getShardIndexingPressureTracker(shardId);

        // Verify updated initial shard limits
        assertEquals(10000, coordinatingShardTracker.getPrimaryAndCoordinatingLimits());
        assertEquals(10000, primaryShardTracker.getPrimaryAndCoordinatingLimits());
        assertEquals(15000, replicaShardTracker.getReplicaLimits());
    }

    public void testShardIndexingPressureLastSuccessfulSettingsUpdate() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        int totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long bulkShardRequestSize = totalRequestSize + (RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class) * 80)
            + RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

        boolean randomBoolean = randomBoolean();

        Settings settings = Settings.builder()
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED.getKey(), true)
            .put(ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENFORCED.getKey(), true)
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), (long) (bulkShardRequestSize * 3.8) + "B")
            .put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 1)
            .put(ShardIndexingPressureMemoryManager.SUCCESSFUL_REQUEST_ELAPSED_TIMEOUT.getKey(), "10ms")
            .build();
        restartCluster(settings);

        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        IndexService indexService = internalCluster().getInstance(IndicesService.class, primaryName).iterator().next();
        Index index = indexService.getIndexSettings().getIndex();
        ShardId shardId = new ShardId(index, 0);

        // Send first request which gets successful to set the last successful time-stamp
        if (randomBoolean) {
            ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            assertNoFailures(successFuture.actionGet());
        } else {
            ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);
            assertNoFailures(successFuture.actionGet());
        }

        // Send couple of more requests which remains outstanding to increase time-stamp value
        ActionFuture<BulkResponse> successFuture;
        ActionFuture<BulkResponse> secondSuccessFuture;
        Releasable primaryReleasable = blockPrimary(primaryName);
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
            secondSuccessFuture = client(primaryName).bulk(bulkRequest);
        }

        // This request breaches the threshold and hence will be rejected
        if (randomBoolean) {
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
            waitForLastSuccessfulRequestTimeout(coordinatingShardTracker);
            assertRejectedBulkResponse(bulkRequest, client(coordinatingOnlyNode).bulk(bulkRequest).actionGet());
            assertEquals(1, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(
                1,
                coordinatingShardTracker.getCoordinatingOperationTracker()
                    .getRejectionTracker()
                    .getLastSuccessfulRequestLimitsBreachedRejections()
            );
        } else {
            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            waitForLastSuccessfulRequestTimeout(primaryShardTracker);
            assertRejectedBulkResponse(bulkRequest, client(primaryName).bulk(bulkRequest).actionGet());
            assertEquals(1, primaryShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(
                1,
                primaryShardTracker.getCoordinatingOperationTracker()
                    .getRejectionTracker()
                    .getLastSuccessfulRequestLimitsBreachedRejections()
            );
        }

        primaryReleasable.close();
        assertNoFailures(successFuture.actionGet());
        assertNoFailures(secondSuccessFuture.actionGet());

        // Update the outstanding threshold setting to see no rejections
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(
            Settings.builder().put(ShardIndexingPressureMemoryManager.MAX_OUTSTANDING_REQUESTS.getKey(), 10)
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // Verify no rejection with similar request pattern
        primaryReleasable = blockPrimary(primaryName);
        if (randomBoolean) {
            successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            secondSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        } else {
            successFuture = client(primaryName).bulk(bulkRequest);
            secondSuccessFuture = client(primaryName).bulk(bulkRequest);
        }
        ActionFuture<BulkResponse> thirdSuccessFuture;
        if (randomBoolean) {
            ShardIndexingPressureTracker coordinatingShardTracker = internalCluster().getInstance(
                IndexingPressureService.class,
                coordinatingOnlyNode
            ).getShardIndexingPressure().getShardIndexingPressureTracker(shardId);
            waitForLastSuccessfulRequestTimeout(coordinatingShardTracker);
            thirdSuccessFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            assertEquals(1, coordinatingShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(
                1,
                coordinatingShardTracker.getCoordinatingOperationTracker()
                    .getRejectionTracker()
                    .getLastSuccessfulRequestLimitsBreachedRejections()
            );
        } else {
            ShardIndexingPressureTracker primaryShardTracker = internalCluster().getInstance(IndexingPressureService.class, primaryName)
                .getShardIndexingPressure()
                .getShardIndexingPressureTracker(shardId);
            waitForLastSuccessfulRequestTimeout(primaryShardTracker);
            thirdSuccessFuture = client(primaryName).bulk(bulkRequest);
            assertEquals(1, primaryShardTracker.getCoordinatingOperationTracker().getRejectionTracker().getTotalRejections());
            assertEquals(
                1,
                primaryShardTracker.getCoordinatingOperationTracker()
                    .getRejectionTracker()
                    .getLastSuccessfulRequestLimitsBreachedRejections()
            );
        }

        primaryReleasable.close();
        assertNoFailures(successFuture.actionGet());
        assertNoFailures(secondSuccessFuture.actionGet());
        thirdSuccessFuture.actionGet();
    }

    private Releasable blockPrimary(String primaryName) {
        final CountDownLatch latchBlockingReplicationSend = new CountDownLatch(1);

        TransportService primaryService = internalCluster().getInstance(TransportService.class, primaryName);
        final MockTransportService primaryTransportService = (MockTransportService) primaryService;

        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportShardBulkAction.ACTION_NAME + "[r]")) {
                try {
                    latchBlockingReplicationSend.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        return () -> {
            if (latchBlockingReplicationSend.getCount() > 0) {
                latchBlockingReplicationSend.countDown();
            }
        };
    }

    private Tuple<String, String> getPrimaryReplicaNodeNames(String indexName) {
        IndicesStatsResponse response = client().admin().indices().prepareStats(indexName).get();
        String primaryId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(ShardRouting::primary)
            .findAny()
            .get()
            .currentNodeId();
        String replicaId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(sr -> sr.primary() == false)
            .findAny()
            .get()
            .currentNodeId();
        DiscoveryNodes nodes = client().admin().cluster().prepareState().get().getState().nodes();
        String primaryName = nodes.get(primaryId).getName();
        String replicaName = nodes.get(replicaId).getName();
        return new Tuple<>(primaryName, replicaName);
    }

    private String getCoordinatingOnlyNode() {
        return client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .nodes()
            .getCoordinatingOnlyNodes()
            .values()
            .iterator()
            .next()
            .getName();
    }

    private static void assertRejectedBulkResponse(BulkRequest request, BulkResponse response) {
        assertEquals(RestStatus.OK, response.status());
        assertTrue(response.hasFailures());
        assertEquals(request.numberOfActions(), response.getItems().length);
        for (int i = 0; i < response.getItems().length; i++) {
            BulkItemResponse item = response.getItems()[i];
            assertEquals(i, item.getItemId());
            assertEquals(request.requests().get(i).index(), item.getIndex());
            assertEquals(request.requests().get(i).id(), item.getId());
            assertEquals(request.requests().get(i).opType(), item.getOpType());
            assertTrue(item.isFailed());
            assertEquals(RestStatus.TOO_MANY_REQUESTS, item.status());
            assertThat(ExceptionsHelper.unwrapCause(item.getFailure().getCause()), instanceOf(OpenSearchRejectedExecutionException.class));
        }
    }

    private static void waitForLastSuccessfulRequestTimeout(ShardIndexingPressureTracker tracker) throws Exception {
        assertBusy(() -> {
            ShardIndexingPressureTracker.PerformanceTracker performanceTracker = tracker.getCoordinatingOperationTracker()
                .getPerformanceTracker();
            assertEquals(2, performanceTracker.getTotalOutstandingRequests());
            long lastSuccessfulRequestTimestamp = performanceTracker.getLastSuccessfulRequestTimestamp();
            assertTrue(lastSuccessfulRequestTimestamp > 0);
            assertTrue(System.nanoTime() - lastSuccessfulRequestTimestamp > TimeUnit.MILLISECONDS.toNanos(10));
        });
    }

    private void restartCluster(Settings settings) throws Exception {
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(unboundedWriteQueue).put(settings).build();
            }
        });
    }
}
