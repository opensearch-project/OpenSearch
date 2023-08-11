/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart;

import org.junit.Before;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemoteStoreMultipartFileCorruptionIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockFsRepositoryPlugin.class)).collect(Collectors.toList());
    }

    @Before
    public void setup() {
        setupRepo();
    }

    protected Settings remoteStoreIndexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put("index.refresh_interval", "300s")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    public void testLocalFileCorruptionDuringUpload() {
        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME, remoteStoreIndexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        indexSingleDoc(INDEX_NAME);

        client().admin()
            .indices()
            .prepareRefresh(INDEX_NAME)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .execute()
            .actionGet();

        // ensuring red cluster meaning shard has failed and is unassigned
        ensureRed(INDEX_NAME);
    }
}
