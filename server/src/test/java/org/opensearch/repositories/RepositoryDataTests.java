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
import org.opensearch.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteStoreEnums.PathType.FIXED;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.opensearch.repositories.RepositoryData.EMPTY_REPO_GEN;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests for the {@link RepositoryData} class.
 */
public class RepositoryDataTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        RepositoryData repositoryData1 = generateRandomRepoData(FIXED.getCode());
        RepositoryData repositoryData2 = repositoryData1.copy();
        assertEquals(repositoryData1, repositoryData2);
        assertEquals(repositoryData1.hashCode(), repositoryData2.hashCode());
    }

    public void testIndicesToUpdateAfterRemovingSnapshot() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final List<IndexId> indicesBefore = new ArrayList<>(repositoryData.getIndices().values());
        final SnapshotId randomSnapshot = randomFrom(repositoryData.getSnapshotIds());
        final IndexId[] indicesToUpdate = indicesBefore.stream().filter(index -> {
            final List<SnapshotId> snapshotIds = repositoryData.getSnapshots(index);
            return snapshotIds.contains(randomSnapshot) && snapshotIds.size() > 1;
        }).toArray(IndexId[]::new);
        assertThat(
            repositoryData.indicesToUpdateAfterRemovingSnapshot(Collections.singleton(randomSnapshot)),
            containsInAnyOrder(indicesToUpdate)
        );
    }

    public void testXContent() throws IOException {
        RepositoryData repositoryData = generateRandomRepoData();
        XContentBuilder builder = JsonXContent.contentBuilder();
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            long gen = (long) randomIntBetween(0, 500);
            RepositoryData fromXContent = RepositoryData.snapshotsFromXContent(parser, gen);
            assertEquals(repositoryData, fromXContent);
            assertEquals(gen, fromXContent.getGenId());
        }
    }

    public void testAddSnapshots() {
        RepositoryData repositoryData = generateRandomRepoData();
        // test that adding the same snapshot id to the repository data throws an exception
        Map<String, IndexId> indexIdMap = repositoryData.getIndices();
        // test that adding a snapshot and its indices works
        SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
        List<IndexId> indices = new ArrayList<>();
        Set<IndexId> newIndices = new HashSet<>();
        int numNew = randomIntBetween(1, 10);
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        for (int i = 0; i < numNew; i++) {
            IndexId indexId = new IndexId(randomAlphaOfLength(7), UUIDs.randomBase64UUID());
            newIndices.add(indexId);
            indices.add(indexId);
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        int numOld = randomIntBetween(1, indexIdMap.size());
        List<String> indexNames = new ArrayList<>(indexIdMap.keySet());
        for (int i = 0; i < numOld; i++) {
            final IndexId indexId = indexIdMap.get(indexNames.get(i));
            indices.add(indexId);
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup = shardGenerations.indices()
            .stream()
            .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
        RepositoryData newRepoData = repositoryData.addSnapshot(
            newSnapshot,
            randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            shardGenerations,
            indexLookup,
            indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
        );
        // verify that the new repository data has the new snapshot and its indices
        assertTrue(newRepoData.getSnapshotIds().contains(newSnapshot));
        for (IndexId indexId : indices) {
            List<SnapshotId> snapshotIds = newRepoData.getSnapshots(indexId);
            assertTrue(snapshotIds.contains(newSnapshot));
            if (newIndices.contains(indexId)) {
                assertEquals(snapshotIds.size(), 1); // if it was a new index, only the new snapshot should be in its set
            }
        }
        assertEquals(repositoryData.getGenId(), newRepoData.getGenId());
    }

    public void testInitIndices() {
        final int numSnapshots = randomIntBetween(1, 30);
        final Map<String, SnapshotId> snapshotIds = new HashMap<>(numSnapshots);
        final Map<String, SnapshotState> snapshotStates = new HashMap<>(numSnapshots);
        final Map<String, Version> snapshotVersions = new HashMap<>(numSnapshots);
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), randomFrom(SnapshotState.values()));
            snapshotVersions.put(snapshotId.getUUID(), randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()));
        }
        RepositoryData repositoryData = new RepositoryData(
            EMPTY_REPO_GEN,
            snapshotIds,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY
        );
        // test that initializing indices works
        Map<IndexId, List<SnapshotId>> indices = randomIndices(snapshotIds);
        RepositoryData newRepoData = new RepositoryData(
            repositoryData.getGenId(),
            snapshotIds,
            snapshotStates,
            snapshotVersions,
            indices,
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY
        );
        List<SnapshotId> expected = new ArrayList<>(repositoryData.getSnapshotIds());
        Collections.sort(expected);
        List<SnapshotId> actual = new ArrayList<>(newRepoData.getSnapshotIds());
        Collections.sort(actual);
        assertEquals(expected, actual);
        for (IndexId indexId : indices.keySet()) {
            assertEquals(indices.get(indexId), newRepoData.getSnapshots(indexId));
        }
    }

    public void testRemoveSnapshot() {
        RepositoryData repositoryData = generateRandomRepoData();
        List<SnapshotId> snapshotIds = new ArrayList<>(repositoryData.getSnapshotIds());
        assertThat(snapshotIds.size(), greaterThan(0));
        SnapshotId removedSnapshotId = snapshotIds.remove(randomIntBetween(0, snapshotIds.size() - 1));
        RepositoryData newRepositoryData = repositoryData.removeSnapshots(Collections.singleton(removedSnapshotId), ShardGenerations.EMPTY);
        // make sure the repository data's indices no longer contain the removed snapshot
        for (final IndexId indexId : newRepositoryData.getIndices().values()) {
            assertFalse(newRepositoryData.getSnapshots(indexId).contains(removedSnapshotId));
        }
    }

    public void testResolveIndexId() {
        RepositoryData repositoryData = generateRandomRepoData();
        Map<String, IndexId> indices = repositoryData.getIndices();
        Set<String> indexNames = indices.keySet();
        assertThat(indexNames.size(), greaterThan(0));
        String indexName = indexNames.iterator().next();
        IndexId indexId = indices.get(indexName);
        assertEquals(indexId, repositoryData.resolveIndexId(indexName));
    }

    public void testGetSnapshotState() {
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
        final SnapshotState state = randomFrom(SnapshotState.values());
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(
            snapshotId,
            state,
            randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
            ShardGenerations.EMPTY,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertEquals(state, repositoryData.getSnapshotState(snapshotId));
        assertNull(repositoryData.getSnapshotState(new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID())));
    }

    public void testIndexThatReferencesAnUnknownSnapshot() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        final RepositoryData repositoryData = generateRandomRepoData();

        XContentBuilder builder = XContentBuilder.builder(xContent);
        repositoryData.snapshotsToXContent(builder, Version.CURRENT);
        RepositoryData parsedRepositoryData;
        try (XContentParser xParser = createParser(builder)) {
            parsedRepositoryData = RepositoryData.snapshotsFromXContent(xParser, repositoryData.getGenId());
        }
        assertEquals(repositoryData, parsedRepositoryData);

        Map<String, SnapshotId> snapshotIds = new HashMap<>();
        Map<String, SnapshotState> snapshotStates = new HashMap<>();
        Map<String, Version> snapshotVersions = new HashMap<>();
        for (SnapshotId snapshotId : parsedRepositoryData.getSnapshotIds()) {
            snapshotIds.put(snapshotId.getUUID(), snapshotId);
            snapshotStates.put(snapshotId.getUUID(), parsedRepositoryData.getSnapshotState(snapshotId));
            snapshotVersions.put(snapshotId.getUUID(), parsedRepositoryData.getVersion(snapshotId));
        }

        final IndexId corruptedIndexId = randomFrom(parsedRepositoryData.getIndices().values());

        Map<IndexId, List<SnapshotId>> indexSnapshots = new HashMap<>();
        final ShardGenerations.Builder shardGenBuilder = ShardGenerations.builder();
        for (Map.Entry<String, IndexId> snapshottedIndex : parsedRepositoryData.getIndices().entrySet()) {
            IndexId indexId = snapshottedIndex.getValue();
            List<SnapshotId> snapshotsIds = new ArrayList<>(parsedRepositoryData.getSnapshots(indexId));
            if (corruptedIndexId.equals(indexId)) {
                snapshotsIds.add(new SnapshotId("_uuid", "_does_not_exist"));
            }
            indexSnapshots.put(indexId, snapshotsIds);
            final int shardCount = randomIntBetween(1, 10);
            for (int i = 0; i < shardCount; ++i) {
                shardGenBuilder.put(indexId, i, UUIDs.randomBase64UUID(random()));
            }
        }
        assertNotNull(corruptedIndexId);

        RepositoryData corruptedRepositoryData = new RepositoryData(
            parsedRepositoryData.getGenId(),
            snapshotIds,
            snapshotStates,
            snapshotVersions,
            indexSnapshots,
            shardGenBuilder.build(),
            IndexMetaDataGenerations.EMPTY
        );

        final XContentBuilder corruptedBuilder = XContentBuilder.builder(xContent);
        corruptedRepositoryData.snapshotsToXContent(corruptedBuilder, Version.CURRENT);

        try (XContentParser xParser = createParser(corruptedBuilder)) {
            OpenSearchParseException e = expectThrows(
                OpenSearchParseException.class,
                () -> RepositoryData.snapshotsFromXContent(xParser, corruptedRepositoryData.getGenId())
            );
            assertThat(
                e.getMessage(),
                equalTo(
                    "Detected a corrupted repository, index "
                        + corruptedIndexId
                        + " references an unknown "
                        + "snapshot uuid [_does_not_exist]"
                )
            );
        }
    }

    public void testIndexThatReferenceANullSnapshot() throws IOException {
        final XContentBuilder builder = MediaTypeRegistry.JSON.contentBuilder();
        builder.startObject();
        {
            builder.startArray("snapshots");
            builder.value(new SnapshotId("_name", "_uuid"));
            builder.endArray();

            builder.startObject("indices");
            {
                builder.startObject("docs");
                {
                    builder.field("id", "_id");
                    builder.startArray("snapshots");
                    {
                        builder.startObject();
                        if (randomBoolean()) {
                            builder.field("name", "_name");
                        }
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        try (XContentParser xParser = createParser(builder)) {
            OpenSearchParseException e = expectThrows(
                OpenSearchParseException.class,
                () -> RepositoryData.snapshotsFromXContent(xParser, randomNonNegativeLong())
            );
            assertThat(
                e.getMessage(),
                equalTo("Detected a corrupted repository, " + "index [docs/_id/0] references an unknown snapshot uuid [null]")
            );
        }
    }

    // Test removing snapshot from random data where no two snapshots share any index metadata blobs
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotNoSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final SnapshotId snapshotId = randomFrom(repositoryData.getSnapshotIds());
        final IndexMetaDataGenerations indexMetaDataGenerations = repositoryData.indexMetaDataGenerations();
        final Collection<IndexId> indicesToUpdate = repositoryData.indicesToUpdateAfterRemovingSnapshot(Collections.singleton(snapshotId));
        final Map<IndexId, Collection<String>> identifiersToRemove = indexMetaDataGenerations.lookup.get(snapshotId)
            .entrySet()
            .stream()
            .filter(e -> indicesToUpdate.contains(e.getKey()))
            .collect(
                Collectors.toMap(Map.Entry::getKey, e -> Collections.singleton(indexMetaDataGenerations.getIndexMetaBlobId(e.getValue())))
            );
        assertEquals(repositoryData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(snapshotId)), identifiersToRemove);
    }

    // Test removing snapshot from random data that has some or all index metadata shared
    public void testIndexMetaDataToRemoveAfterRemovingSnapshotWithSharing() {
        final RepositoryData repositoryData = generateRandomRepoData();
        final ShardGenerations.Builder builder = ShardGenerations.builder();
        final SnapshotId otherSnapshotId = randomFrom(repositoryData.getSnapshotIds());
        final Collection<IndexId> indicesInOther = repositoryData.getIndices()
            .values()
            .stream()
            .filter(index -> repositoryData.getSnapshots(index).contains(otherSnapshotId))
            .collect(Collectors.toSet());
        for (IndexId indexId : indicesInOther) {
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final Map<IndexId, String> newIndices = new HashMap<>();
        final Map<String, String> newIdentifiers = new HashMap<>();
        final Map<IndexId, Collection<String>> removeFromOther = new HashMap<>();
        for (IndexId indexId : randomSubsetOf(repositoryData.getIndices().values())) {
            if (indicesInOther.contains(indexId)) {
                removeFromOther.put(
                    indexId,
                    Collections.singleton(repositoryData.indexMetaDataGenerations().indexMetaBlobId(otherSnapshotId, indexId))
                );
            }
            final String identifier = randomAlphaOfLength(20);
            newIndices.put(indexId, identifier);
            newIdentifiers.put(identifier, UUIDs.randomBase64UUID(random()));
            builder.put(indexId, 0, UUIDs.randomBase64UUID(random()));
        }
        final ShardGenerations shardGenerations = builder.build();
        final Map<IndexId, String> indexLookup = new HashMap<>(repositoryData.indexMetaDataGenerations().lookup.get(otherSnapshotId));
        indexLookup.putAll(newIndices);
        final SnapshotId newSnapshot = new SnapshotId(randomAlphaOfLength(7), UUIDs.randomBase64UUID(random()));

        RepositoryData newRepoData = repositoryData.addSnapshot(
            newSnapshot,
            SnapshotState.SUCCESS,
            Version.CURRENT,
            shardGenerations,
            indexLookup,
            newIdentifiers
        );
        assertEquals(
            newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(newSnapshot)),
            newIndices.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.singleton(newIdentifiers.get(e.getValue()))))
        );
        assertEquals(newRepoData.indexMetaDataToRemoveAfterRemovingSnapshots(Collections.singleton(otherSnapshotId)), removeFromOther);
    }

    public void testResolveNewIndices() {
        // Test case 1: All indices are new
        List<String> indicesToResolve = Arrays.asList("index1", "index2", "index3");
        Map<String, IndexId> inFlightIds = Collections.emptyMap();
        int pathType = randomIntBetween(0, 2);
        List<IndexId> resolvedIndices = RepositoryData.EMPTY.resolveNewIndices(indicesToResolve, inFlightIds, pathType);
        assertEquals(indicesToResolve.size(), resolvedIndices.size());
        for (IndexId indexId : resolvedIndices) {
            assertTrue(indicesToResolve.contains(indexId.getName()));
            assertNotNull(indexId.getId());
            assertEquals(pathType, indexId.getShardPathType());
        }

        // Test case 2: Some indices are existing, some are new
        RepositoryData repositoryData = generateRandomRepoData();
        Map<String, IndexId> existingIndices = repositoryData.getIndices();
        List<String> existingIndexNames = new ArrayList<>(existingIndices.keySet());
        List<String> newIndexNames = Arrays.asList("newIndex1", "newIndex2");
        indicesToResolve = new ArrayList<>(existingIndexNames);
        indicesToResolve.addAll(newIndexNames);
        pathType = randomIntBetween(0, 2);
        resolvedIndices = repositoryData.resolveNewIndices(indicesToResolve, Collections.emptyMap(), pathType);
        assertEquals(indicesToResolve.size(), resolvedIndices.size());
        for (IndexId indexId : resolvedIndices) {
            if (existingIndexNames.contains(indexId.getName())) {
                assertEquals(existingIndices.get(indexId.getName()), indexId);
            } else {
                assertTrue(newIndexNames.contains(indexId.getName()));
                assertNotNull(indexId.getId());
                assertEquals(pathType, indexId.getShardPathType());
            }
        }

        // Test case 3: Some indices are in-flight
        Map<String, IndexId> inFlightIndexIds = new HashMap<>();
        for (String indexName : newIndexNames) {
            inFlightIndexIds.put(indexName, new IndexId(indexName, UUIDs.randomBase64UUID(), pathType));
        }
        resolvedIndices = repositoryData.resolveNewIndices(indicesToResolve, inFlightIndexIds, pathType);
        assertEquals(indicesToResolve.size(), resolvedIndices.size());
        for (IndexId indexId : resolvedIndices) {
            if (existingIndexNames.contains(indexId.getName())) {
                assertEquals(existingIndices.get(indexId.getName()), indexId);
            } else if (newIndexNames.contains(indexId.getName())) {
                assertEquals(inFlightIndexIds.get(indexId.getName()), indexId);
            } else {
                fail("Unexpected index: " + indexId.getName());
            }
        }
    }

    public void testResolveNewIndicesWithDifferentPathType() {
        // Generate repository data with a fixed path type
        int existingPathType = PathType.FIXED.getCode();
        RepositoryData repositoryData = generateRandomRepoData(existingPathType);
        Map<String, IndexId> existingIndices = repositoryData.getIndices();

        // Create a list of existing and new index names
        List<String> existingIndexNames = new ArrayList<>(existingIndices.keySet());
        List<String> newIndexNames = Arrays.asList("newIndex1", "newIndex2");
        List<String> indicesToResolve = new ArrayList<>(existingIndexNames);
        indicesToResolve.addAll(newIndexNames);

        // Use a different path type for new indices
        int newPathType = HASHED_PREFIX.getCode();

        List<IndexId> resolvedIndices = repositoryData.resolveNewIndices(indicesToResolve, Collections.emptyMap(), newPathType);
        assertEquals(indicesToResolve.size(), resolvedIndices.size());
        for (IndexId indexId : resolvedIndices) {
            if (existingIndexNames.contains(indexId.getName())) {
                assertEquals(existingIndices.get(indexId.getName()), indexId);
                assertEquals(existingPathType, indexId.getShardPathType());
            } else {
                assertTrue(newIndexNames.contains(indexId.getName()));
                assertNotNull(indexId.getId());
                assertEquals(newPathType, indexId.getShardPathType());
            }
        }
    }

    public static RepositoryData generateRandomRepoData() {
        return generateRandomRepoData(randomFrom(PathType.values()).getCode());
    }

    public static RepositoryData generateRandomRepoData(int pathType) {
        final int numIndices = randomIntBetween(1, 30);
        final List<IndexId> indices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indices.add(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID(), pathType));
        }
        final int numSnapshots = randomIntBetween(1, 30);
        RepositoryData repositoryData = RepositoryData.EMPTY;
        for (int i = 0; i < numSnapshots; i++) {
            final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final List<IndexId> someIndices = indices.subList(0, randomIntBetween(1, numIndices));
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (IndexId someIndex : someIndices) {
                final int shardCount = randomIntBetween(1, 10);
                for (int j = 0; j < shardCount; ++j) {
                    final String uuid = randomBoolean() ? null : UUIDs.randomBase64UUID(random());
                    builder.put(someIndex, j, uuid);
                }
            }
            final Map<IndexId, String> indexLookup = someIndices.stream()
                .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            repositoryData = repositoryData.addSnapshot(
                snapshotId,
                randomFrom(SnapshotState.values()),
                randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()),
                builder.build(),
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
            );
        }
        return repositoryData;
    }

    private static Map<IndexId, List<SnapshotId>> randomIndices(final Map<String, SnapshotId> snapshotIdsMap) {
        final List<SnapshotId> snapshotIds = new ArrayList<>(snapshotIdsMap.values());
        final int totalSnapshots = snapshotIds.size();
        final int numIndices = randomIntBetween(1, 30);
        final Map<IndexId, List<SnapshotId>> indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            final IndexId indexId = new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            final Set<SnapshotId> indexSnapshots = new LinkedHashSet<>();
            final int numIndicesForSnapshot = randomIntBetween(1, numIndices);
            for (int j = 0; j < numIndicesForSnapshot; j++) {
                indexSnapshots.add(snapshotIds.get(randomIntBetween(0, totalSnapshots - 1)));
            }
            indices.put(indexId, Collections.unmodifiableList(new ArrayList<>(indexSnapshots)));
        }
        return indices;
    }
}
