/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.create;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexDefaultReplicaCountWithNodeSettingsIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-index";
    private static final int DEFAULT_NUMBER_OF_REPLICAS = 0;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey(), DEFAULT_NUMBER_OF_REPLICAS)
            .build();
    }

    @Override
    protected void randomIndexTemplate() {
        // The base template sets index.number_of_replicas and would override the cluster-level default verified here.
    }

    public void testCreateIndexUsesDefaultNumberOfReplicasFromNodeSettings() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1))
                .get()
        );

        Settings indexSettings = client().admin().indices().prepareGetSettings(INDEX_NAME).get().getIndexToSettings().get(INDEX_NAME);
        assertThat(indexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, null), equalTo(DEFAULT_NUMBER_OF_REPLICAS));
    }
}
