/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.block;

import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VerifiableWriteable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexModule;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;
import static org.opensearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;

/**
 * Represents current cluster level blocks to block dirty operations done against the cluster.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterBlocks extends AbstractDiffable<ClusterBlocks> implements VerifiableWriteable {
    public static final ClusterBlocks EMPTY_CLUSTER_BLOCK = new ClusterBlocks(emptySet(), Map.of());
    public static final Set<Setting<Boolean>> INDEX_DATA_READ_ONLY_BLOCK_SETTINGS = Set.of(
        IndexMetadata.INDEX_READ_ONLY_SETTING,
        IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
        IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING
    );
    private final Set<ClusterBlock> global;

    private final Map<String, Set<ClusterBlock>> indicesBlocks;

    private final EnumMap<ClusterBlockLevel, ImmutableLevelHolder> levelHolders;

    ClusterBlocks(Set<ClusterBlock> global, final Map<String, Set<ClusterBlock>> indicesBlocks) {
        this.global = global;
        this.indicesBlocks = Collections.unmodifiableMap(indicesBlocks);
        levelHolders = generateLevelHolders(global, indicesBlocks);
    }

    public Set<ClusterBlock> global() {
        return global;
    }

    public Map<String, Set<ClusterBlock>> indices() {
        return indicesBlocks;
    }

    public Set<ClusterBlock> global(ClusterBlockLevel level) {
        return levelHolders.get(level).global();
    }

    public Map<String, Set<ClusterBlock>> indices(ClusterBlockLevel level) {
        return levelHolders.get(level).indices();
    }

    private Set<ClusterBlock> blocksForIndex(ClusterBlockLevel level, String index) {
        return indices(level).getOrDefault(index, emptySet());
    }

    private static EnumMap<ClusterBlockLevel, ImmutableLevelHolder> generateLevelHolders(
        Set<ClusterBlock> global,
        final Map<String, Set<ClusterBlock>> indicesBlocks
    ) {

        EnumMap<ClusterBlockLevel, ImmutableLevelHolder> levelHolders = new EnumMap<>(ClusterBlockLevel.class);
        for (final ClusterBlockLevel level : ClusterBlockLevel.values()) {
            Predicate<ClusterBlock> containsLevel = block -> block.contains(level);
            Set<ClusterBlock> newGlobal = unmodifiableSet(global.stream().filter(containsLevel).collect(toSet()));

            final Map<String, Set<ClusterBlock>> indicesBuilder = new HashMap<>();
            for (final Map.Entry<String, Set<ClusterBlock>> entry : indicesBlocks.entrySet()) {
                indicesBuilder.put(entry.getKey(), unmodifiableSet(entry.getValue().stream().filter(containsLevel).collect(toSet())));
            }
            levelHolders.put(level, new ImmutableLevelHolder(newGlobal, indicesBuilder));
        }
        return levelHolders;
    }

    /**
     * Returns {@code true} if one of the global blocks as its disable state persistence flag set.
     */
    public boolean disableStatePersistence() {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.disableStatePersistence()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasGlobalBlock(ClusterBlock block) {
        return global.contains(block);
    }

    public boolean hasGlobalBlockWithId(final int blockId) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.id() == blockId) {
                return true;
            }
        }
        return false;
    }

    public boolean hasGlobalBlockWithLevel(ClusterBlockLevel level) {
        return global(level).size() > 0;
    }

    /**
     * Is there a global block with the provided status?
     */
    public boolean hasGlobalBlockWithStatus(final RestStatus status) {
        for (ClusterBlock clusterBlock : global) {
            if (clusterBlock.status().equals(status)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasIndexBlock(String index, ClusterBlock block) {
        return indicesBlocks.containsKey(index) && indicesBlocks.get(index).contains(block);
    }

    public boolean hasIndexBlockWithId(String index, int blockId) {
        final Set<ClusterBlock> clusterBlocks = indicesBlocks.get(index);
        if (clusterBlocks != null) {
            for (ClusterBlock clusterBlock : clusterBlocks) {
                if (clusterBlock.id() == blockId) {
                    return true;
                }
            }
        }
        return false;
    }

    @Nullable
    public ClusterBlock getIndexBlockWithId(final String index, final int blockId) {
        final Set<ClusterBlock> clusterBlocks = indicesBlocks.get(index);
        if (clusterBlocks != null) {
            for (ClusterBlock clusterBlock : clusterBlocks) {
                if (clusterBlock.id() == blockId) {
                    return clusterBlock;
                }
            }
        }
        return null;
    }

    public void globalBlockedRaiseException(ClusterBlockLevel level) throws ClusterBlockException {
        ClusterBlockException blockException = globalBlockedException(level);
        if (blockException != null) {
            throw blockException;
        }
    }

    private boolean globalBlocked(ClusterBlockLevel level) {
        return global(level).isEmpty() == false;
    }

    public ClusterBlockException globalBlockedException(ClusterBlockLevel level) {
        if (globalBlocked(level) == false) {
            return null;
        }
        return new ClusterBlockException(global(level));
    }

    public IndexCreateBlockException createIndexBlockedException(ClusterBlockLevel level) {
        if (!globalBlocked(level)) {
            return null;
        }
        return new IndexCreateBlockException(global(level));
    }

    public void indexBlockedRaiseException(ClusterBlockLevel level, String index) throws ClusterBlockException {
        ClusterBlockException blockException = indexBlockedException(level, index);
        if (blockException != null) {
            throw blockException;
        }
    }

    public ClusterBlockException indexBlockedException(ClusterBlockLevel level, String index) {
        return indicesBlockedException(level, new String[] { index });
    }

    public boolean indexBlocked(ClusterBlockLevel level, String index) {
        return globalBlocked(level) || blocksForIndex(level, index).isEmpty() == false;
    }

    public ClusterBlockException indicesBlockedException(ClusterBlockLevel level, String[] indices) {
        Set<ClusterBlock> globalLevelBlocks = global(level);
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();
        for (String index : indices) {
            Set<ClusterBlock> indexBlocks = blocksForIndex(level, index);
            if (indexBlocks.isEmpty() == false || globalLevelBlocks.isEmpty() == false) {
                indexLevelBlocks.put(index, Sets.union(indexBlocks, globalLevelBlocks));
            }
        }
        if (indexLevelBlocks.isEmpty()) {
            if (globalLevelBlocks.isEmpty() == false) {
                return new ClusterBlockException(globalLevelBlocks);
            }
            return null;
        }
        return new ClusterBlockException(indexLevelBlocks);
    }

    /**
     * Returns <code>true</code> iff non of the given have a {@link ClusterBlockLevel#METADATA_WRITE} in place where the
     * {@link ClusterBlock#isAllowReleaseResources()} returns <code>false</code>. This is used in places where resources will be released
     * like the deletion of an index to free up resources on nodes.
     * @param indices the indices to check
     */

    public ClusterBlockException indicesAllowReleaseResources(String[] indices) {
        Set<ClusterBlock> globalBlocks = global(ClusterBlockLevel.METADATA_WRITE).stream()
            .filter(clusterBlock -> clusterBlock.isAllowReleaseResources() == false)
            .collect(toSet());
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();
        for (String index : indices) {
            Set<ClusterBlock> blocks = Sets.union(globalBlocks, blocksForIndex(ClusterBlockLevel.METADATA_WRITE, index))
                .stream()
                .filter(clusterBlock -> clusterBlock.isAllowReleaseResources() == false)
                .collect(toSet());
            if (blocks.isEmpty() == false) {
                indexLevelBlocks.put(index, Sets.union(globalBlocks, blocks));
            }
        }
        if (indexLevelBlocks.isEmpty()) {
            if (globalBlocks.isEmpty() == false) {
                return new ClusterBlockException(globalBlocks);
            }
            return null;
        }
        return new ClusterBlockException(indexLevelBlocks);
    }

    public static ClusterBlockException indicesWithRemoteSnapshotBlockedException(Collection<String> concreteIndices, ClusterState state) {
        for (String index : concreteIndices) {
            if (state.blocks().indexBlocked(ClusterBlockLevel.METADATA_WRITE, index)) {
                IndexMetadata indexMeta = state.metadata().index(index);
                if (indexMeta != null
                    && (IndexModule.Type.REMOTE_SNAPSHOT.match(indexMeta.getSettings().get(INDEX_STORE_TYPE_SETTING.getKey())) == false
                        || ClusterBlocks.INDEX_DATA_READ_ONLY_BLOCK_SETTINGS.stream()
                            .anyMatch(booleanSetting -> booleanSetting.exists(indexMeta.getSettings())))) {
                    return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices.toArray(new String[0]));
                }
            }
        }
        return null;
    }

    @Override
    public String toString() {
        if (global.isEmpty() && indices().isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("blocks: \n");
        if (global.isEmpty() == false) {
            sb.append("   _global_:\n");
            for (ClusterBlock block : global) {
                sb.append("      ").append(block);
            }
        }
        for (final Map.Entry<String, Set<ClusterBlock>> entry : indices().entrySet()) {
            sb.append("   ").append(entry.getKey()).append(":\n");
            for (ClusterBlock block : entry.getValue()) {
                sb.append("      ").append(block);
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeBlockSet(global, out);
        out.writeMap(indicesBlocks, StreamOutput::writeString, (o, s) -> writeBlockSet(s, o));
    }

    @Override
    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        writeTo(out);
    }

    private static void writeBlockSet(Set<ClusterBlock> blocks, StreamOutput out) throws IOException {
        out.writeCollection(blocks);
    }

    public static ClusterBlocks readFrom(StreamInput in) throws IOException {
        final Set<ClusterBlock> global = readBlockSet(in);
        final Map<String, Set<ClusterBlock>> indicesBlocks = in.readMap(i -> i.readString().intern(), ClusterBlocks::readBlockSet);
        if (global.isEmpty() && indicesBlocks.isEmpty()) {
            return EMPTY_CLUSTER_BLOCK;
        }
        return new ClusterBlocks(global, indicesBlocks);
    }

    private static Set<ClusterBlock> readBlockSet(StreamInput in) throws IOException {
        final Set<ClusterBlock> blocks = in.readSet(ClusterBlock::new);
        return blocks.isEmpty() ? blocks : unmodifiableSet(blocks);
    }

    public static Diff<ClusterBlocks> readDiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(ClusterBlocks::readFrom, in);
    }

    /**
     * An immutable level holder.
     *
     * @opensearch.internal
     */
    static class ImmutableLevelHolder {

        private final Set<ClusterBlock> global;
        private final Map<String, Set<ClusterBlock>> indices;

        ImmutableLevelHolder(Set<ClusterBlock> global, final Map<String, Set<ClusterBlock>> indices) {
            this.global = global;
            this.indices = Collections.unmodifiableMap(indices);
        }

        public Set<ClusterBlock> global() {
            return global;
        }

        public Map<String, Set<ClusterBlock>> indices() {
            return indices;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for cluster blocks.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Set<ClusterBlock> global = new HashSet<>();

        private final Map<String, Set<ClusterBlock>> indices = new HashMap<>();

        public Builder() {}

        public Builder blocks(ClusterBlocks blocks) {
            global.addAll(blocks.global());
            for (final Map.Entry<String, Set<ClusterBlock>> entry : blocks.indices().entrySet()) {
                if (!indices.containsKey(entry.getKey())) {
                    indices.put(entry.getKey(), new HashSet<>());
                }
                indices.get(entry.getKey()).addAll(entry.getValue());
            }
            return this;
        }

        public Builder addBlocks(IndexMetadata indexMetadata) {
            String indexName = indexMetadata.getIndex().getName();
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                addIndexBlock(indexName, MetadataIndexStateService.INDEX_CLOSED_BLOCK);
            }
            if (IndexMetadata.INDEX_READ_ONLY_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_READ_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(indexName, IndexMetadata.INDEX_READ_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_METADATA_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(indexName, IndexMetadata.INDEX_METADATA_BLOCK);
            }
            if (IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.get(indexMetadata.getSettings())) {
                addIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            }
            if (indexMetadata.isRemoteSnapshot()) {
                addIndexBlock(indexName, IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE);
            }
            return this;
        }

        public Builder updateBlocks(IndexMetadata indexMetadata) {
            // let's remove all blocks for this index and add them back -- no need to remove all individual blocks....
            indices.remove(indexMetadata.getIndex().getName());
            return addBlocks(indexMetadata);
        }

        public Builder addGlobalBlock(ClusterBlock block) {
            global.add(block);
            return this;
        }

        public Builder removeGlobalBlock(ClusterBlock block) {
            global.remove(block);
            return this;
        }

        public Builder removeGlobalBlock(int blockId) {
            global.removeIf(block -> block.id() == blockId);
            return this;
        }

        public Builder addIndexBlock(String index, ClusterBlock block) {
            if (!indices.containsKey(index)) {
                indices.put(index, new HashSet<>());
            }
            indices.get(index).add(block);
            return this;
        }

        public Builder removeIndexBlocks(String index) {
            if (!indices.containsKey(index)) {
                return this;
            }
            indices.remove(index);
            return this;
        }

        public boolean hasIndexBlock(String index, ClusterBlock block) {
            return indices.getOrDefault(index, Collections.emptySet()).contains(block);
        }

        public Builder removeIndexBlock(String index, ClusterBlock block) {
            if (!indices.containsKey(index)) {
                return this;
            }
            indices.get(index).remove(block);
            if (indices.get(index).isEmpty()) {
                indices.remove(index);
            }
            return this;
        }

        public Builder removeIndexBlockWithId(String index, int blockId) {
            final Set<ClusterBlock> indexBlocks = indices.get(index);
            if (indexBlocks == null) {
                return this;
            }
            indexBlocks.removeIf(block -> block.id() == blockId);
            if (indexBlocks.isEmpty()) {
                indices.remove(index);
            }
            return this;
        }

        public ClusterBlocks build() {
            if (indices.isEmpty() && global.isEmpty()) {
                return EMPTY_CLUSTER_BLOCK;
            }
            // We copy the block sets here in case of the builder is modified after build is called
            final HashMap<String, Set<ClusterBlock>> indicesBuilder = new HashMap<>(indices.size());
            for (Map.Entry<String, Set<ClusterBlock>> entry : indices.entrySet()) {
                indicesBuilder.put(entry.getKey(), unmodifiableSet(new HashSet<>(entry.getValue())));
            }
            return new ClusterBlocks(unmodifiableSet(new HashSet<>(global)), indicesBuilder);
        }
    }
}
