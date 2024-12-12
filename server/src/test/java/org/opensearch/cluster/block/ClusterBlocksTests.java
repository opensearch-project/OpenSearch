/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.block;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.cluster.block.ClusterBlockTests.randomClusterBlock;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_METADATA_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_READ_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_WRITE_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;

public class ClusterBlocksTests extends OpenSearchTestCase {

    public void testWriteVerifiableTo() throws Exception {
        ClusterBlock clusterBlock1 = randomClusterBlock();
        ClusterBlock clusterBlock2 = randomClusterBlock();
        ClusterBlock clusterBlock3 = randomClusterBlock();

        ClusterBlocks clusterBlocks = ClusterBlocks.builder()
            .addGlobalBlock(clusterBlock1)
            .addGlobalBlock(clusterBlock2)
            .addGlobalBlock(clusterBlock3)
            .addIndexBlock("index-1", clusterBlock1)
            .addIndexBlock("index-2", clusterBlock2)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        clusterBlocks.writeVerifiableTo(checksumOut);
        StreamInput in = out.bytes().streamInput();
        ClusterBlocks result = ClusterBlocks.readFrom(in);

        assertEquals(clusterBlocks.global().size(), result.global().size());
        assertEquals(clusterBlocks.global(), result.global());
        assertEquals(clusterBlocks.indices().size(), result.indices().size());
        assertEquals(clusterBlocks.indices(), result.indices());

        ClusterBlocks clusterBlocks2 = ClusterBlocks.builder()
            .addGlobalBlock(clusterBlock3)
            .addGlobalBlock(clusterBlock1)
            .addGlobalBlock(clusterBlock2)
            .addIndexBlock("index-2", clusterBlock2)
            .addIndexBlock("index-1", clusterBlock1)
            .build();
        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        clusterBlocks2.writeVerifiableTo(checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }

    public void testGlobalBlock() {
        String index = "test-000001";
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        Set<String> indices = new HashSet<>();
        indices.add(index);

        // no global blocks
        {
            stateBuilder.blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK);
            ClusterState clusterState = stateBuilder.build();
            clusterState.blocks();
            assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, clusterState));
        }

        // has global block
        {
            for (ClusterBlock block : Arrays.asList(
                INDEX_READ_ONLY_BLOCK,
                INDEX_READ_BLOCK,
                INDEX_WRITE_BLOCK,
                INDEX_METADATA_BLOCK,
                INDEX_READ_ONLY_ALLOW_DELETE_BLOCK,
                REMOTE_READ_ONLY_ALLOW_DELETE
            )) {
                stateBuilder.blocks(ClusterBlocks.builder().addGlobalBlock(block).build());
                ClusterState clusterState = stateBuilder.build();
                clusterState.blocks();
                assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, clusterState));
            }
        }
    }

    public void testIndexWithBlock() {
        String index = "test-000001";
        Set<String> indices = new HashSet<>();
        indices.add(index);
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
        stateBuilder.blocks(ClusterBlocks.builder().addIndexBlock(index, IndexMetadata.INDEX_METADATA_BLOCK));
        stateBuilder.metadata(Metadata.builder().put(createIndexMetadata(index, false, null, null), false));
        ClusterState clusterState = stateBuilder.build();
        clusterState.blocks();
        assertNotNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, stateBuilder.build()));
    }

    public void testRemoteIndexBlock() {
        String remoteIndex = "remote_index";
        Set<String> indices = new HashSet<>();
        indices.add(remoteIndex);
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));

        {
            IndexMetadata remoteSnapshotIndexMetadata = createIndexMetadata(remoteIndex, true, null, null);
            stateBuilder.metadata(Metadata.builder().put(remoteSnapshotIndexMetadata, false));
            stateBuilder.blocks(ClusterBlocks.builder().addBlocks(remoteSnapshotIndexMetadata));

            ClusterState clusterState = stateBuilder.build();
            assertTrue(clusterState.blocks().hasIndexBlock(remoteIndex, IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE));
            clusterState.blocks();
            assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, clusterState));
        }

        // searchable snapshot index with block
        {
            Setting<Boolean> setting = RandomizedTest.randomFrom(new ArrayList<>(ClusterBlocks.INDEX_DATA_READ_ONLY_BLOCK_SETTINGS));
            IndexMetadata remoteSnapshotIndexMetadata = createIndexMetadata(remoteIndex, true, null, setting);
            stateBuilder.metadata(Metadata.builder().put(remoteSnapshotIndexMetadata, false));
            stateBuilder.blocks(ClusterBlocks.builder().addBlocks(remoteSnapshotIndexMetadata));
            ClusterState clusterState = stateBuilder.build();
            clusterState.blocks();
            assertNotNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, clusterState));
        }
    }

    public void testRemoteIndexWithoutBlock() {
        String remoteIndex = "remote_index";
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));

        String alias = "alias1";
        IndexMetadata remoteSnapshotIndexMetadata = createIndexMetadata(remoteIndex, true, alias, null);
        String index = "test-000001";
        IndexMetadata indexMetadata = createIndexMetadata(index, false, alias, null);
        stateBuilder.metadata(Metadata.builder().put(remoteSnapshotIndexMetadata, false).put(indexMetadata, false));

        Set<String> indices = new HashSet<>();
        indices.add(remoteIndex);
        ClusterState clusterState = stateBuilder.build();
        clusterState.blocks();
        assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(indices, clusterState));
    }

    public void testRemoteIndexWithIndexBlock() {
        String index = "test-000001";
        String remoteIndex = "remote_index";
        String alias = "alias1";
        {
            ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
            IndexMetadata remoteSnapshotIndexMetadata = createIndexMetadata(remoteIndex, true, alias, null);
            IndexMetadata indexMetadata = createIndexMetadata(index, false, alias, null);
            stateBuilder.metadata(Metadata.builder().put(remoteSnapshotIndexMetadata, false).put(indexMetadata, false))
                .blocks(ClusterBlocks.builder().addBlocks(remoteSnapshotIndexMetadata));
            ClusterState clusterState = stateBuilder.build();
            clusterState.blocks();
            assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(Collections.singleton(index), clusterState));
            clusterState.blocks();
            assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(Collections.singleton(remoteIndex), clusterState));
        }

        {
            ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName("cluster"));
            Setting<Boolean> setting = RandomizedTest.randomFrom(new ArrayList<>(ClusterBlocks.INDEX_DATA_READ_ONLY_BLOCK_SETTINGS));
            IndexMetadata remoteSnapshotIndexMetadata = createIndexMetadata(remoteIndex, true, alias, setting);
            IndexMetadata indexMetadata = createIndexMetadata(index, false, alias, null);
            stateBuilder.metadata(Metadata.builder().put(remoteSnapshotIndexMetadata, false).put(indexMetadata, false))
                .blocks(ClusterBlocks.builder().addBlocks(remoteSnapshotIndexMetadata));
            ClusterState clusterState = stateBuilder.build();
            assertNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(Collections.singleton(index), clusterState));
            assertNotNull(ClusterBlocks.indicesWithRemoteSnapshotBlockedException(Collections.singleton(remoteIndex), clusterState));
        }
    }

    private IndexMetadata createIndexMetadata(String index, boolean isRemoteIndex, String alias, Setting<Boolean> blockSetting) {
        IndexMetadata.Builder builder = IndexMetadata.builder(index).settings(createIndexSettingBuilder(isRemoteIndex, blockSetting));
        if (alias != null) {
            AliasMetadata.Builder aliasBuilder = AliasMetadata.builder(alias);
            return builder.putAlias(aliasBuilder.build()).build();
        }
        return builder.build();
    }

    private Settings.Builder createIndexSettingBuilder(boolean isRemoteIndex, Setting<Boolean> blockSetting) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "abc")
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1);

        if (isRemoteIndex) {
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                .put(IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY.getKey(), "repo")
                .put(IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME.getKey(), "snapshot");
        }
        if (blockSetting != null) {
            builder.put(blockSetting.getKey(), true);
        }

        return builder;
    }
}
