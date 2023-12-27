/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.alias;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.opensearch.Version;
import org.opensearch.action.RequestValidators;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexAliasesService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.index.alias.RandomAliasActionsGenerator.randomAliasAction;
import static org.mockito.Mockito.mock;

public class TransportIndicesAliasesActionTests extends OpenSearchTestCase {

    public void testIndexWithoutBlock() {
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        stateBuilder.blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK);
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(randomAliasAction());

        assertNull(createAction().checkBlock(request, stateBuilder.build()));
    }

    public void testIndexWithBlock() {
        String index = "test-000001";
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        stateBuilder.blocks(ClusterBlocks.builder().addIndexBlock(index, IndexMetadata.INDEX_METADATA_BLOCK));
        stateBuilder.metadata(Metadata.builder().put(createIndexMetadata(index, false), false));
        IndicesAliasesRequest.AliasActions action = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .aliases("test")
            .indices(index);
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(action);

        assertNotNull(createAction().checkBlock(request, stateBuilder.build()));
    }

    public void testRemoteIndexWithoutBlock() {
        String index = "remote_index";
        IndexMetadata indexMetadata = createIndexMetadata(index, true);
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        stateBuilder.metadata(Metadata.builder().put(indexMetadata, false));
        stateBuilder.blocks(ClusterBlocks.builder().addBlocks(indexMetadata));
        IndicesAliasesRequest.AliasActions action = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
            .aliases("remote")
            .indices(index);
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(action);

        assertNull(createAction().checkBlock(request, stateBuilder.build()));
    }

    public void testRemoteIndexWithBlock() {
        String index = "remote_index";
        IndexMetadata.Builder metaBuilder = IndexMetadata.builder(index);
        Settings.Builder settingsBuilder = createIndexSettingBuilder(true);
        Setting<Boolean> setting = RandomizedTest.randomFrom(
            new ArrayList<>(TransportIndicesAliasesAction.REMOTE_SNAPSHOT_SETTINGS_CHECKLIST)
        );
        settingsBuilder.put(setting.getKey(), true);
        IndexMetadata indexMetadata = metaBuilder.settings(settingsBuilder).build();
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        stateBuilder.metadata(Metadata.builder().put(indexMetadata, false));
        stateBuilder.blocks(ClusterBlocks.builder().addBlocks(indexMetadata));
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(
            IndicesAliasesRequest.AliasActions.Type.ADD
        ).alias("remote").indices(index);
        IndicesAliasesRequest request = new IndicesAliasesRequest().addAliasAction(aliasActions);

        assertNotNull(createAction().checkBlock(request, stateBuilder.build()));
    }

    private IndexMetadata createIndexMetadata(String index, boolean isRemoteIndex) {
        return IndexMetadata.builder(index).settings(createIndexSettingBuilder(isRemoteIndex)).build();
    }

    private Settings.Builder createIndexSettingBuilder(boolean isRemoteIndex) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "abc")
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1);

        if (isRemoteIndex) {
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT)
                .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), "repo")
                .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), "snapshot");
        }

        return builder;
    }

    private TransportIndicesAliasesAction createAction() {
        return new TransportIndicesAliasesAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(MetadataIndexAliasesService.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            new RequestValidators<IndicesAliasesRequest>(null)
        );
    }
}
