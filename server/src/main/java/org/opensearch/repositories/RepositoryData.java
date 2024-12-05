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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.repositories;

import org.opensearch.OpenSearchParseException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class that represents the data in a repository, as captured in the
 * repository's index blob.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RepositoryData {

    /**
     * The generation value indicating the repository has no index generational files.
     */
    public static final long EMPTY_REPO_GEN = -1L;

    /**
     * The generation value indicating that the repository generation is unknown.
     */
    public static final long UNKNOWN_REPO_GEN = -2L;

    /**
     * The generation value indicating that the repository generation could not be determined.
     */
    public static final long CORRUPTED_REPO_GEN = -3L;

    /**
     * An instance initialized for an empty repository.
     */
    public static final RepositoryData EMPTY = new RepositoryData(
        EMPTY_REPO_GEN,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        ShardGenerations.EMPTY,
        IndexMetaDataGenerations.EMPTY
    );

    /**
     * The generational id of the index file from which the repository data was read.
     */
    private final long genId;
    /**
     * The ids of the snapshots in the repository.
     */
    private final Map<String, SnapshotId> snapshotIds;
    /**
     * The states of each snapshot in the repository.
     */
    private final Map<String, SnapshotState> snapshotStates;
    /**
     * The indices found in the repository across all snapshots, as a name to {@link IndexId} mapping
     */
    private final Map<String, IndexId> indices;

    public Map<IndexId, List<SnapshotId>> getIndexSnapshots() {
        return indexSnapshots;
    }

    /**
     * The snapshots that each index belongs to.
     */
    private final Map<IndexId, List<SnapshotId>> indexSnapshots;

    private final Map<String, Version> snapshotVersions;

    /**
     * Index metadata generations.
     */
    private final IndexMetaDataGenerations indexMetaDataGenerations;

    /**
     * Shard generations.
     */
    private final ShardGenerations shardGenerations;

    public RepositoryData(
        long genId,
        Map<String, SnapshotId> snapshotIds,
        Map<String, SnapshotState> snapshotStates,
        Map<String, Version> snapshotVersions,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        ShardGenerations shardGenerations,
        IndexMetaDataGenerations indexMetaDataGenerations
    ) {
        this(
            genId,
            Collections.unmodifiableMap(snapshotIds),
            Collections.unmodifiableMap(snapshotStates),
            Collections.unmodifiableMap(snapshotVersions),
            Collections.unmodifiableMap(indexSnapshots.keySet().stream().collect(Collectors.toMap(IndexId::getName, Function.identity()))),
            Collections.unmodifiableMap(indexSnapshots),
            shardGenerations,
            indexMetaDataGenerations
        );
    }

    private RepositoryData(
        long genId,
        Map<String, SnapshotId> snapshotIds,
        Map<String, SnapshotState> snapshotStates,
        Map<String, Version> snapshotVersions,
        Map<String, IndexId> indices,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        ShardGenerations shardGenerations,
        IndexMetaDataGenerations indexMetaDataGenerations
    ) {
        this.genId = genId;
        this.snapshotIds = snapshotIds;
        this.snapshotStates = snapshotStates;
        this.indices = indices;
        this.indexSnapshots = indexSnapshots;
        this.shardGenerations = shardGenerations;
        this.indexMetaDataGenerations = indexMetaDataGenerations;
        this.snapshotVersions = snapshotVersions;
        assert indices.values().containsAll(shardGenerations.indices()) : "ShardGenerations contained indices "
            + shardGenerations.indices()
            + " but snapshots only reference indices "
            + indices.values();
        assert indexSnapshots.values().stream().noneMatch(snapshotIdList -> new HashSet<>(snapshotIdList).size() != snapshotIdList.size())
            : "Found duplicate snapshot ids per index in [" + indexSnapshots + "]";
    }

    protected RepositoryData copy() {
        return new RepositoryData(
            genId,
            snapshotIds,
            snapshotStates,
            snapshotVersions,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations
        );
    }

    /**
     * Creates a copy of this instance that contains updated version data.
     * @param versions map of snapshot versions
     * @return copy with updated version data
     */
    public RepositoryData withVersions(Map<SnapshotId, Version> versions) {
        if (versions.isEmpty()) {
            return this;
        }
        final Map<String, Version> newVersions = new HashMap<>(snapshotVersions);
        versions.forEach((id, version) -> newVersions.put(id.getUUID(), version));
        return new RepositoryData(
            genId,
            snapshotIds,
            snapshotStates,
            newVersions,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations
        );
    }

    public ShardGenerations shardGenerations() {
        return shardGenerations;
    }

    /**
     * Gets the generational index file id from which this instance was read.
     */
    public long getGenId() {
        return genId;
    }

    /**
     * Returns an unmodifiable collection of the snapshot ids.
     */
    public Collection<SnapshotId> getSnapshotIds() {
        return snapshotIds.values();
    }

    /**
     * Returns the {@link SnapshotState} for the given snapshot.  Returns {@code null} if
     * there is no state for the snapshot.
     */
    @Nullable
    public SnapshotState getSnapshotState(final SnapshotId snapshotId) {
        return snapshotStates.get(snapshotId.getUUID());
    }

    /**
     * Returns the {@link Version} for the given snapshot or {@code null} if unknown.
     */
    public Version getVersion(SnapshotId snapshotId) {
        return snapshotVersions.get(snapshotId.getUUID());
    }

    /**
     * Returns an unmodifiable map of the index names to {@link IndexId} in the repository.
     */
    public Map<String, IndexId> getIndices() {
        return indices;
    }

    /**
     * Returns the list of {@link IndexId} that have their snapshots updated but not removed (because they are still referenced by other
     * snapshots) after removing the given snapshot from the repository.
     *
     * @param snapshotIds SnapshotId to remove
     * @return List of indices that are changed but not removed
     */
    public List<IndexId> indicesToUpdateAfterRemovingSnapshot(Collection<SnapshotId> snapshotIds) {
        return indexSnapshots.entrySet().stream().filter(entry -> {
            final Collection<SnapshotId> existingIds = entry.getValue();
            if (snapshotIds.containsAll(existingIds)) {
                return existingIds.size() > snapshotIds.size();
            }
            for (SnapshotId snapshotId : snapshotIds) {
                if (entry.getValue().contains(snapshotId)) {
                    return true;
                }
            }
            return false;
        }).map(Map.Entry::getKey).collect(Collectors.toList());
    }

    /**
     * Returns a map of {@link IndexId} to a collection of {@link String} containing all the {@link IndexId} and the
     * {@link org.opensearch.cluster.metadata.IndexMetadata} blob name in it that can be removed after removing the given snapshot from
     * the repository.
     * NOTE: Does not return a mapping for {@link IndexId} values that will be removed completely from the repository.
     *
     * @param snapshotIds SnapshotIds to remove
     * @return map of index to index metadata blob id to delete
     */
    public Map<IndexId, Collection<String>> indexMetaDataToRemoveAfterRemovingSnapshots(Collection<SnapshotId> snapshotIds) {
        Collection<IndexId> indicesForSnapshot = indicesToUpdateAfterRemovingSnapshot(snapshotIds);
        final Set<String> allRemainingIdentifiers = indexMetaDataGenerations.lookup.entrySet()
            .stream()
            .filter(e -> snapshotIds.contains(e.getKey()) == false)
            .flatMap(e -> e.getValue().values().stream())
            .map(indexMetaDataGenerations::getIndexMetaBlobId)
            .collect(Collectors.toSet());
        final Map<IndexId, Collection<String>> toRemove = new HashMap<>();
        for (IndexId indexId : indicesForSnapshot) {
            for (SnapshotId snapshotId : snapshotIds) {
                final String identifier = indexMetaDataGenerations.indexMetaBlobId(snapshotId, indexId);
                if (allRemainingIdentifiers.contains(identifier) == false) {
                    toRemove.computeIfAbsent(indexId, k -> new HashSet<>()).add(identifier);
                }
            }
        }
        return toRemove;
    }

    /**
     * Add a snapshot and its indices to the repository; returns a new instance.  If the snapshot
     * already exists in the repository data, this method throws an IllegalArgumentException.
     *
     * @param snapshotId       Id of the new snapshot
     * @param snapshotState    State of the new snapshot
     * @param shardGenerations Updated shard generations in the new snapshot. For each index contained in the snapshot an array of new
     *                         generations indexed by the shard id they correspond to must be supplied.
     * @param indexMetaBlobs   Map of index metadata blob uuids
     * @param newIdentifiers   Map of new index metadata blob uuids keyed by the identifiers of the
     *                         {@link org.opensearch.cluster.metadata.IndexMetadata} in them
     */
    public RepositoryData addSnapshot(
        final SnapshotId snapshotId,
        final SnapshotState snapshotState,
        final Version version,
        final ShardGenerations shardGenerations,
        @Nullable final Map<IndexId, String> indexMetaBlobs,
        @Nullable final Map<String, String> newIdentifiers
    ) {
        if (snapshotIds.containsKey(snapshotId.getUUID())) {
            // if the snapshot id already exists in the repository data, it means an old master
            // that is blocked from the cluster is trying to finalize a snapshot concurrently with
            // the new master, so we make the operation idempotent
            return this;
        }
        Map<String, SnapshotId> snapshots = new HashMap<>(snapshotIds);
        snapshots.put(snapshotId.getUUID(), snapshotId);
        Map<String, SnapshotState> newSnapshotStates = new HashMap<>(snapshotStates);
        newSnapshotStates.put(snapshotId.getUUID(), snapshotState);
        Map<String, Version> newSnapshotVersions = new HashMap<>(snapshotVersions);
        newSnapshotVersions.put(snapshotId.getUUID(), version);
        Map<IndexId, List<SnapshotId>> allIndexSnapshots = new HashMap<>(indexSnapshots);
        for (final IndexId indexId : shardGenerations.indices()) {
            final List<SnapshotId> snapshotIds = allIndexSnapshots.get(indexId);
            if (snapshotIds == null) {
                allIndexSnapshots.put(indexId, Collections.singletonList(snapshotId));
            } else {
                final List<SnapshotId> copy = new ArrayList<>(snapshotIds.size() + 1);
                copy.addAll(snapshotIds);
                copy.add(snapshotId);
                allIndexSnapshots.put(indexId, Collections.unmodifiableList(copy));
            }
        }

        final IndexMetaDataGenerations newIndexMetaGenerations;
        if (indexMetaBlobs == null) {
            assert newIdentifiers == null : "Non-null new identifiers [" + newIdentifiers + "] for null lookup";
            assert indexMetaDataGenerations.lookup.isEmpty() : "Index meta generations should have been empty but was ["
                + indexMetaDataGenerations
                + "]";
            newIndexMetaGenerations = IndexMetaDataGenerations.EMPTY;
        } else {
            assert indexMetaBlobs.isEmpty() || shardGenerations.indices().equals(indexMetaBlobs.keySet())
                : "Shard generations contained indices "
                    + shardGenerations.indices()
                    + " but indexMetaData was given for "
                    + indexMetaBlobs.keySet();
            newIndexMetaGenerations = indexMetaDataGenerations.withAddedSnapshot(snapshotId, indexMetaBlobs, newIdentifiers);
        }

        return new RepositoryData(
            genId,
            snapshots,
            newSnapshotStates,
            newSnapshotVersions,
            allIndexSnapshots,
            ShardGenerations.builder().putAll(this.shardGenerations).putAll(shardGenerations).build(),
            newIndexMetaGenerations
        );
    }

    /**
     * Create a new instance with the given generation and all other fields equal to this instance.
     *
     * @param newGeneration New Generation
     * @return New instance
     */
    public RepositoryData withGenId(long newGeneration) {
        if (newGeneration == genId) {
            return this;
        }
        return new RepositoryData(
            newGeneration,
            snapshotIds,
            snapshotStates,
            snapshotVersions,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations
        );
    }

    /**
     * Remove snapshots and remove any indices that no longer exist in the repository due to the deletion of the snapshots.
     *
     * @param snapshots               Snapshot ids to remove
     * @param updatedShardGenerations Shard generations that changed as a result of removing the snapshot.
     *                                The {@code String[]} passed for each {@link IndexId} contains the new shard generation id for each
     *                                changed shard indexed by its shardId
     */
    public RepositoryData removeSnapshots(final Collection<SnapshotId> snapshots, final ShardGenerations updatedShardGenerations) {
        Map<String, SnapshotId> newSnapshotIds = snapshotIds.values()
            .stream()
            .filter(sn -> snapshots.contains(sn) == false)
            .collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        if (newSnapshotIds.size() != snapshotIds.size() - snapshots.size()) {
            final Collection<SnapshotId> notFound = new HashSet<>(snapshots);
            notFound.removeAll(snapshotIds.values());
            throw new ResourceNotFoundException("Attempting to remove non-existent snapshots {} from repository data", notFound);
        }
        Map<String, SnapshotState> newSnapshotStates = new HashMap<>(snapshotStates);
        final Map<String, Version> newSnapshotVersions = new HashMap<>(snapshotVersions);
        for (SnapshotId snapshotId : snapshots) {
            newSnapshotStates.remove(snapshotId.getUUID());
            newSnapshotVersions.remove(snapshotId.getUUID());
        }
        Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        for (final IndexId indexId : indices.values()) {
            List<SnapshotId> snapshotIds = this.indexSnapshots.get(indexId);
            assert snapshotIds != null;
            List<SnapshotId> remaining = new ArrayList<>(snapshotIds);
            if (remaining.removeAll(snapshots)) {
                remaining = Collections.unmodifiableList(remaining);
            } else {
                remaining = snapshotIds;
            }
            if (remaining.isEmpty() == false) {
                indexSnapshots.put(indexId, remaining);
            }
        }

        return new RepositoryData(
            genId,
            newSnapshotIds,
            newSnapshotStates,
            newSnapshotVersions,
            indexSnapshots,
            ShardGenerations.builder()
                .putAll(shardGenerations)
                .putAll(updatedShardGenerations)
                .retainIndicesAndPruneDeletes(indexSnapshots.keySet())
                .build(),
            indexMetaDataGenerations.withRemovedSnapshots(snapshots)
        );
    }

    /**
     * Returns an immutable collection of the snapshot ids for the snapshots that contain the given index.
     */
    public List<SnapshotId> getSnapshots(final IndexId indexId) {
        List<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
        if (snapshotIds == null) {
            throw new IllegalArgumentException("unknown snapshot index " + indexId);
        }
        return snapshotIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RepositoryData that = (RepositoryData) obj;
        return snapshotIds.equals(that.snapshotIds)
            && snapshotStates.equals(that.snapshotStates)
            && snapshotVersions.equals(that.snapshotVersions)
            && indices.equals(that.indices)
            && indexSnapshots.equals(that.indexSnapshots)
            && shardGenerations.equals(that.shardGenerations)
            && indexMetaDataGenerations.equals(that.indexMetaDataGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            snapshotIds,
            snapshotStates,
            snapshotVersions,
            indices,
            indexSnapshots,
            shardGenerations,
            indexMetaDataGenerations
        );
    }

    /**
     * Resolve the index name to the index id specific to the repository,
     * throwing an exception if the index could not be resolved.
     */
    public IndexId resolveIndexId(final String indexName) {
        return Objects.requireNonNull(indices.get(indexName), () -> "Tried to resolve unknown index [" + indexName + "]");
    }

    /**
     * Resolve the given index names to index ids.
     */
    public List<IndexId> resolveIndices(final List<String> indices) {
        List<IndexId> resolvedIndices = new ArrayList<>(indices.size());
        for (final String indexName : indices) {
            resolvedIndices.add(resolveIndexId(indexName));
        }
        return resolvedIndices;
    }

    /**
     * Resolve the given index names to index ids, creating new index ids for
     * new indices in the repository.
     *
     * @param indicesToResolve names of indices to resolve
     * @param inFlightIds      name to index mapping for currently in-flight snapshots not yet in the repository data to fall back to
     */
    public List<IndexId> resolveNewIndices(List<String> indicesToResolve, Map<String, IndexId> inFlightIds, int pathType) {
        List<IndexId> snapshotIndices = new ArrayList<>();
        for (String index : indicesToResolve) {
            IndexId indexId = indices.get(index);
            if (indexId == null) {
                indexId = inFlightIds.get(index);
            }
            if (indexId == null) {
                indexId = new IndexId(index, UUIDs.randomBase64UUID(), pathType);
            }
            snapshotIndices.add(indexId);
        }
        return snapshotIndices;
    }

    private static final String SHARD_GENERATIONS = "shard_generations";
    private static final String INDEX_METADATA_IDENTIFIERS = "index_metadata_identifiers";
    private static final String INDEX_METADATA_LOOKUP = "index_metadata_lookup";
    private static final String SNAPSHOTS = "snapshots";
    private static final String INDICES = "indices";
    private static final String INDEX_ID = "id";
    private static final String NAME = "name";
    private static final String UUID = "uuid";
    private static final String STATE = "state";
    private static final String VERSION = "version";
    private static final String MIN_VERSION = "min_version";

    // Visible for testing only
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final Version repoMetaVersion) throws IOException {
        return snapshotsToXContent(builder, repoMetaVersion, Version.V_2_17_0);
    }

    /**
     * Writes the snapshots metadata and the related indices metadata to x-content.
     */
    public XContentBuilder snapshotsToXContent(final XContentBuilder builder, final Version repoMetaVersion, final Version minNodeVersion)
        throws IOException {
        builder.startObject();
        // write the snapshots list
        builder.startArray(SNAPSHOTS);
        for (final SnapshotId snapshot : getSnapshotIds()) {
            builder.startObject();
            builder.field(NAME, snapshot.getName());
            final String snapshotUUID = snapshot.getUUID();
            builder.field(UUID, snapshotUUID);
            final SnapshotState state = snapshotStates.get(snapshotUUID);
            if (state != null) {
                builder.field(STATE, state.value());
            }
            builder.startObject(INDEX_METADATA_LOOKUP);
            for (Map.Entry<IndexId, String> entry : indexMetaDataGenerations.lookup.getOrDefault(snapshot, Collections.emptyMap())
                .entrySet()) {
                builder.field(entry.getKey().getId(), entry.getValue());
            }
            builder.endObject();
            final Version version = snapshotVersions.get(snapshotUUID);
            if (version != null) {
                builder.field(VERSION, version.toString());
            }
            builder.endObject();
        }
        builder.endArray();
        // write the indices map
        builder.startObject(INDICES);
        for (final IndexId indexId : getIndices().values()) {
            builder.startObject(indexId.getName());
            builder.field(INDEX_ID, indexId.getId());
            if (minNodeVersion.onOrAfter(Version.V_2_17_0)) {
                builder.field(IndexId.SHARD_PATH_TYPE, indexId.getShardPathType());
            }
            builder.startArray(SNAPSHOTS);
            List<SnapshotId> snapshotIds = indexSnapshots.get(indexId);
            assert snapshotIds != null;
            for (final SnapshotId snapshotId : snapshotIds) {
                builder.value(snapshotId.getUUID());
            }
            builder.endArray();
            builder.startArray(SHARD_GENERATIONS);
            for (String gen : shardGenerations.getGens(indexId)) {
                builder.value(gen);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.field(INDEX_METADATA_IDENTIFIERS, indexMetaDataGenerations.identifiers);
        builder.endObject();
        return builder;
    }

    public IndexMetaDataGenerations indexMetaDataGenerations() {
        return indexMetaDataGenerations;
    }

    /**
     * Reads an instance of {@link RepositoryData} from x-content, loading the snapshots and indices metadata.
     */
    public static RepositoryData snapshotsFromXContent(XContentParser parser, long genId) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        final Map<String, SnapshotId> snapshots = new HashMap<>();
        final Map<String, SnapshotState> snapshotStates = new HashMap<>();
        final Map<String, Version> snapshotVersions = new HashMap<>();
        final Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final Map<String, IndexId> indexLookup = new HashMap<>();
        final ShardGenerations.Builder shardGenerations = ShardGenerations.builder();
        final Map<SnapshotId, Map<String, String>> indexMetaLookup = new HashMap<>();
        Map<String, String> indexMetaIdentifiers = null;
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            final String field = parser.currentName();
            switch (field) {
                case SNAPSHOTS:
                    parseSnapshots(parser, snapshots, snapshotStates, snapshotVersions, indexMetaLookup);
                    break;
                case INDICES:
                    parseIndices(parser, snapshots, indexSnapshots, indexLookup, shardGenerations);
                    break;
                case INDEX_METADATA_IDENTIFIERS:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    indexMetaIdentifiers = parser.mapStrings();
                    break;
                case MIN_VERSION:
                    // ignore min_version
                    // todo: remove in next version
                    parser.nextToken();
                    break;
                default:
                    XContentParserUtils.throwUnknownField(field, parser.getTokenLocation());
            }
        }

        return new RepositoryData(
            genId,
            snapshots,
            snapshotStates,
            snapshotVersions,
            indexSnapshots,
            shardGenerations.build(),
            buildIndexMetaGenerations(indexMetaLookup, indexLookup, indexMetaIdentifiers)
        );
    }

    /**
     * Builds {@link IndexMetaDataGenerations} instance from the information parsed previously.
     *
     * @param indexMetaLookup      map of {@link SnapshotId} to map of index id (as returned by {@link IndexId#getId}) that defines the
     *                             index metadata generations for the snapshot that was parsed by {@link #parseSnapshots}
     * @param indexLookup          map of index uuid (as returned by {@link IndexId#getId}) to {@link IndexId} that was parsed by
     *                             {@link #parseIndices}
     * @param indexMetaIdentifiers map of index generation to index meta identifiers parsed by {@link #snapshotsFromXContent}
     * @return index meta generations instance
     */
    private static IndexMetaDataGenerations buildIndexMetaGenerations(
        Map<SnapshotId, Map<String, String>> indexMetaLookup,
        Map<String, IndexId> indexLookup,
        Map<String, String> indexMetaIdentifiers
    ) {
        if (indexMetaLookup.isEmpty()) {
            return IndexMetaDataGenerations.EMPTY;
        }
        // Build a new map that instead of indexing the per-snapshot index generations by index id string, is indexed by IndexId
        final Map<SnapshotId, Map<IndexId, String>> indexGenerations = new HashMap<>(indexMetaLookup.size());
        for (Map.Entry<SnapshotId, Map<String, String>> snapshotIdMapEntry : indexMetaLookup.entrySet()) {
            final Map<String, String> val = snapshotIdMapEntry.getValue();
            final Map<IndexId, String> forSnapshot = new HashMap<>(val.size());
            for (Map.Entry<String, String> generationEntry : val.entrySet()) {
                forSnapshot.put(indexLookup.get(generationEntry.getKey()), generationEntry.getValue());
            }
            indexGenerations.put(snapshotIdMapEntry.getKey(), forSnapshot);
        }
        return new IndexMetaDataGenerations(indexGenerations, indexMetaIdentifiers);
    }

    /**
     * Parses the "snapshots" field and fills maps for the various per snapshot properties. This method must run before
     * {@link #parseIndices} which will rely on the maps of snapshot properties to have been populated already.
     *
     * @param parser           x-content parse
     * @param snapshots        map of snapshot uuid to {@link SnapshotId}
     * @param snapshotStates   map of snapshot uuid to {@link SnapshotState}
     * @param snapshotVersions map of snapshot uuid to {@link Version} that the snapshot was taken in
     * @param indexMetaLookup  map of {@link SnapshotId} to map of index id (as returned by {@link IndexId#getId}) that defines the index
     *                         metadata generations for the snapshot
     */
    private static void parseSnapshots(
        XContentParser parser,
        Map<String, SnapshotId> snapshots,
        Map<String, SnapshotState> snapshotStates,
        Map<String, Version> snapshotVersions,
        Map<SnapshotId, Map<String, String>> indexMetaLookup
    ) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
        final Map<String, String> stringDeduplicator = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            String name = null;
            String uuid = null;
            SnapshotState state = null;
            Map<String, String> metaGenerations = null;
            Version version = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                switch (currentFieldName) {
                    case NAME:
                        name = parser.text();
                        break;
                    case UUID:
                        uuid = parser.text();
                        break;
                    case STATE:
                        state = SnapshotState.fromValue((byte) parser.intValue());
                        break;
                    case INDEX_METADATA_LOOKUP:
                        metaGenerations = parser.map(HashMap::new, p -> stringDeduplicator.computeIfAbsent(p.text(), Function.identity()));
                        break;
                    case VERSION:
                        version = Version.fromString(parser.text());
                        break;
                }
            }
            final SnapshotId snapshotId = new SnapshotId(name, uuid);
            if (state != null) {
                snapshotStates.put(uuid, state);
            }
            if (version != null) {
                snapshotVersions.put(uuid, version);
            }
            snapshots.put(uuid, snapshotId);
            if (metaGenerations != null && metaGenerations.isEmpty() == false) {
                indexMetaLookup.put(snapshotId, metaGenerations);
            }
        }
    }

    /**
     * Parses information about all indices tracked in the repository and populates {@code indexSnapshots}, {@code indexLookup} and
     * {@code shardGenerations}.
     *
     * @param parser              x-content parser
     * @param snapshots           map of snapshot uuid to {@link SnapshotId} that was populated by {@link #parseSnapshots}
     * @param indexSnapshots      map of {@link IndexId} to list of {@link SnapshotId} that contain the given index
     * @param indexLookup         map of index uuid (as returned by {@link IndexId#getId}) to {@link IndexId}
     * @param shardGenerations    shard generations builder that is populated index by this method
     */
    private static void parseIndices(
        XContentParser parser,
        Map<String, SnapshotId> snapshots,
        Map<IndexId, List<SnapshotId>> indexSnapshots,
        Map<String, IndexId> indexLookup,
        ShardGenerations.Builder shardGenerations
    ) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            final String indexName = parser.currentName();
            final List<SnapshotId> snapshotIds = new ArrayList<>();
            final List<String> gens = new ArrayList<>();

            String id = null;
            int pathType = IndexId.DEFAULT_SHARD_PATH_TYPE;
            IndexId indexId = null;

            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                final String indexMetaFieldName = parser.currentName();
                final XContentParser.Token currentToken = parser.nextToken();
                switch (indexMetaFieldName) {
                    case INDEX_ID:
                        id = parser.text();
                        break;
                    case IndexId.SHARD_PATH_TYPE:
                        pathType = parser.intValue();
                        break;
                    case SNAPSHOTS:
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, currentToken, parser);
                        XContentParser.Token currToken;
                        while ((currToken = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            final String uuid;
                            // the old format pre 5.4.1 which contains the snapshot name and uuid
                            if (currToken == XContentParser.Token.START_OBJECT) {
                                uuid = parseLegacySnapshotUUID(parser);
                            } else {
                                // the new format post 5.4.1 that only contains the snapshot uuid,
                                // since we already have the name/uuid combo in the snapshots array
                                uuid = parser.text();
                            }

                            final SnapshotId snapshotId = snapshots.get(uuid);
                            if (snapshotId == null) {
                                // A snapshotted index references a snapshot which does not exist in
                                // the list of snapshots. This can happen when multiple clusters in
                                // different versions create or delete snapshot in the same repository.
                                throw new OpenSearchParseException(
                                    "Detected a corrupted repository, index "
                                        + new IndexId(indexName, id, pathType)
                                        + " references an unknown snapshot uuid ["
                                        + uuid
                                        + "]"
                                );
                            }
                            snapshotIds.add(snapshotId);
                        }
                        break;
                    case SHARD_GENERATIONS:
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, currentToken, parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            gens.add(parser.textOrNull());
                        }
                        break;
                }
            }
            assert id != null;
            indexId = new IndexId(indexName, id, pathType);
            indexSnapshots.put(indexId, Collections.unmodifiableList(snapshotIds));
            indexLookup.put(id, indexId);
            for (int i = 0; i < gens.size(); i++) {
                String parsedGen = gens.get(i);
                if (parsedGen != null) {
                    shardGenerations.put(indexId, i, parsedGen);
                }
            }
        }
    }

    private static String parseLegacySnapshotUUID(XContentParser parser) throws IOException {
        String uuid = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String currentFieldName = parser.currentName();
            parser.nextToken();
            if (UUID.equals(currentFieldName)) {
                uuid = parser.text();
            }
        }
        return uuid;
    }
}
