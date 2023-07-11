/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.junit.Before;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class SegmentReplicationSuiteIT extends SegmentReplicationBaseIT {

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        createIndex(INDEX_NAME);
    }

    @Override
    public Settings indexSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(super.indexSettings())
            // reset shard & replica count to random values set by OpenSearchIntegTestCase.
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);

        // TODO: Randomly enable remote store on these tests.
        return builder.build();
    }

    public void testBasicReplication() throws Exception {
        final int docCount = scaledRandomIntBetween(10, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh();
        ensureGreen(INDEX_NAME);
        verifyStoreContent();
    }

    public void testDropRandomNodeDuringReplication() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        internalCluster().startClusterManagerOnlyNodes(1);

        final int docCount = scaledRandomIntBetween(10, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh();

        internalCluster().restartRandomDataNode();

        ensureYellow(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId(Integer.toString(docCount)).setSource("field", "value" + docCount).execute().get();
        internalCluster().startDataOnlyNode();
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testDeleteIndexWhileReplicating() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int docCount = scaledRandomIntBetween(10, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).actionGet();
    }

    public void testFullRestartDuringReplication() throws Exception {
        internalCluster().startNode();
        final int docCount = scaledRandomIntBetween(10, 200);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        refresh(INDEX_NAME);
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);
    }
}
