/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.opensearch.ratelimitting.admissioncontrol.AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.INDEXING_CPU_USAGE_LIMIT;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.SEARCH_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class AdmissionControlMultiNodeIT extends OpenSearchIntegTestCase {

    public static final Settings settings = Settings.builder()
        .put(ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
        .put(ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueMillis(500))
        .put(ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), AdmissionControlMode.ENFORCED)
        .put(SEARCH_CPU_USAGE_LIMIT.getKey(), 0)
        .put(INDEXING_CPU_USAGE_LIMIT.getKey(), 0)
        .build();

    private static final Logger LOGGER = LogManager.getLogger(AdmissionControlMultiNodeIT.class);

    public static final String INDEX_NAME = "test_index";

    @Before
    public void init() {
        assertAcked(
            prepareCreate(
                INDEX_NAME,
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );
        ensureGreen(INDEX_NAME);
    }

    @After
    public void cleanup() {
        client().admin().indices().prepareDelete(INDEX_NAME).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(settings).build();
    }

    public void testAdmissionControlRejectionOnEnforced() {
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();
        AdmissionControlService admissionControlServicePrimary = internalCluster().getInstance(AdmissionControlService.class, primaryName);
        AdmissionControlService admissionControlServiceReplica = internalCluster().getInstance(AdmissionControlService.class, replicaName);
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }
        BulkResponse res = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
        assertEquals(429, res.getItems()[0].getFailure().getStatus().getStatus());
        AdmissionControllerStats admissionControlPrimaryStats = admissionControlServicePrimary.stats()
            .getAdmissionControllerStatsList()
            .get(0);
        assertEquals(admissionControlPrimaryStats.rejectionCount.get(AdmissionControlActionType.INDEXING.getType()).longValue(), 1);
        Arrays.stream(res.getItems()).forEach(bulkItemResponse -> {
            assertTrue(bulkItemResponse.getFailureMessage().contains("OpenSearchRejectedExecutionException"));
        });
        SearchResponse searchResponse;
        try {
            searchResponse = client(coordinatingOnlyNode).prepareSearch(INDEX_NAME).get();
        } catch (Exception exception) {
            assertTrue(((SearchPhaseExecutionException) exception).getDetailedMessage().contains("OpenSearchRejectedExecutionException"));
        }
        AdmissionControllerStats primaryStats = admissionControlServicePrimary.stats().getAdmissionControllerStatsList().get(0);
        assertEquals(primaryStats.rejectionCount.get(AdmissionControlActionType.SEARCH.getType()).longValue(), 1);
    }

    public void testAdmissionControlEnforcedOnNonACEnabledActions() throws ExecutionException, InterruptedException {
        String coordinatingOnlyNode = getCoordinatingOnlyNode();
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();

        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(
                    CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                    AdmissionControlMode.ENFORCED.getMode()
                )
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        NodesStatsRequest nodesStatsRequest = new NodesStatsRequest();
        nodesStatsRequest.clear()
            .indices(true)
            .addMetrics(
                NodesStatsRequest.Metric.JVM.metricName(),
                NodesStatsRequest.Metric.OS.metricName(),
                NodesStatsRequest.Metric.FS.metricName(),
                NodesStatsRequest.Metric.PROCESS.metricName(),
                NodesStatsRequest.Metric.ADMISSION_CONTROL.metricName()
            );
        NodesStatsResponse nodesStatsResponse = client(coordinatingOnlyNode).admin().cluster().nodesStats(nodesStatsRequest).actionGet();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().health(new ClusterHealthRequest()).actionGet();
        assertEquals(200, clusterHealthResponse.status().getStatus());
        assertFalse(nodesStatsResponse.hasFailures());
    }

    public void testAdmissionControlRejectionOnMonitor() {
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        AdmissionControlService admissionControlServicePrimary = internalCluster().getInstance(AdmissionControlService.class, primaryName);
        AdmissionControlService admissionControlServiceReplica = internalCluster().getInstance(AdmissionControlService.class, replicaName);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();

        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(
                    CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                    AdmissionControlMode.MONITOR.getMode()
                )
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }
        BulkResponse res = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControllerStats admissionControlPrimaryStats = admissionControlServicePrimary.stats()
            .getAdmissionControllerStatsList()
            .get(0);
        AdmissionControllerStats admissionControlReplicaStats = admissionControlServiceReplica.stats()
            .getAdmissionControllerStatsList()
            .get(0);
        long primaryRejectionCount = admissionControlPrimaryStats.rejectionCount.getOrDefault(
            AdmissionControlActionType.INDEXING.getType(),
            new AtomicLong(0).longValue()
        );
        long replicaRejectionCount = admissionControlReplicaStats.rejectionCount.getOrDefault(
            AdmissionControlActionType.INDEXING.getType(),
            new AtomicLong(0).longValue()
        );
        assertEquals(primaryRejectionCount, 1);
        assertEquals(replicaRejectionCount, 0);
        SearchResponse searchResponse;
        searchResponse = client(coordinatingOnlyNode).prepareSearch(INDEX_NAME).get();
        admissionControlPrimaryStats = admissionControlServicePrimary.stats().getAdmissionControllerStatsList().get(0);
        admissionControlReplicaStats = admissionControlServiceReplica.stats().getAdmissionControllerStatsList().get(0);
        primaryRejectionCount = admissionControlPrimaryStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.SEARCH.getType(), new AtomicLong(0).longValue());
        replicaRejectionCount = admissionControlReplicaStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.SEARCH.getType(), new AtomicLong(0).longValue());
        assertTrue(primaryRejectionCount == 1 || replicaRejectionCount == 1);
        assertFalse(primaryRejectionCount == 1 && replicaRejectionCount == 1);
    }

    public void testAdmissionControlRejectionOnDisabled() {
        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames(INDEX_NAME);
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        AdmissionControlService admissionControlServicePrimary = internalCluster().getInstance(AdmissionControlService.class, primaryName);
        AdmissionControlService admissionControlServiceReplica = internalCluster().getInstance(AdmissionControlService.class, replicaName);

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();

        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(
                    CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                    AdmissionControlMode.DISABLED.getMode()
                )
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }
        BulkResponse res = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
        assertFalse(res.hasFailures());
        AdmissionControllerStats admissionControlPrimaryStats = admissionControlServicePrimary.stats()
            .getAdmissionControllerStatsList()
            .get(0);
        AdmissionControllerStats admissionControlReplicaStats = admissionControlServiceReplica.stats()
            .getAdmissionControllerStatsList()
            .get(0);
        long primaryRejectionCount = admissionControlPrimaryStats.rejectionCount.getOrDefault(
            AdmissionControlActionType.INDEXING.getType(),
            new AtomicLong(0).longValue()
        );
        long replicaRejectionCount = admissionControlReplicaStats.rejectionCount.getOrDefault(
            AdmissionControlActionType.INDEXING.getType(),
            new AtomicLong(0).longValue()
        );
        assertEquals(primaryRejectionCount, 0);
        assertEquals(replicaRejectionCount, 0);
        SearchResponse searchResponse;
        searchResponse = client(coordinatingOnlyNode).prepareSearch(INDEX_NAME).get();
        admissionControlPrimaryStats = admissionControlServicePrimary.stats().getAdmissionControllerStatsList().get(0);
        admissionControlReplicaStats = admissionControlServiceReplica.stats().getAdmissionControllerStatsList().get(0);
        primaryRejectionCount = admissionControlPrimaryStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.SEARCH.getType(), new AtomicLong(0).longValue());
        replicaRejectionCount = admissionControlReplicaStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.SEARCH.getType(), new AtomicLong(0).longValue());
        assertTrue(primaryRejectionCount == 0 && replicaRejectionCount == 0);
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
}
