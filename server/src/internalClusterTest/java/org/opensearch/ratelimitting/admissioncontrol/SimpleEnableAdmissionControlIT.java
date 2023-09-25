/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collections;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class SimpleEnableAdmissionControlIT extends OpenSearchIntegTestCase {

    public static final Settings settings = Settings.builder()
        .put(AdmissionControlSettings.ADMISSION_CONTROL_TRANSPORT_LAYER_MODE.getKey(), "enforced")
        .put(CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), "enforced")
        .build();

    public static final String INDEX_NAME = "test_index";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(settings).build();
    }

    public void testAdmissionControlRejectionFlow() {
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

        AdmissionControlService admissionControlServicePrimary = internalCluster().getInstance(AdmissionControlService.class, primaryName);
        AdmissionControlService admissionControlServiceReplica = internalCluster().getInstance(AdmissionControlService.class, replicaName);
        final BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 3; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }

        // evaluating the admission control triggered or not
        ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().get(0).getRejectionCount(), 1);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().get(0).getRejectionCount(), 0);

        // evaluating the rejection to increment
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().get(0).getRejectionCount(), 2);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().get(0).getRejectionCount(), 0);

        // evaluating the rejection count if the admission controller disabled
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), "disabled")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().get(0).getRejectionCount(), 2);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().get(0).getRejectionCount(), 0);

        // evaluating the rejection count if the admission controller is dynamically enabled
        updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.transientSettings(
            Settings.builder()
                .put(CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), "enforced")
        );
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
        successFuture.actionGet();

        assertEquals(admissionControlServicePrimary.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServicePrimary.getAdmissionControllers().get(0).getRejectionCount(), 3);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().size(), 1);
        assertEquals(admissionControlServiceReplica.getAdmissionControllers().get(0).getRejectionCount(), 0);
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
