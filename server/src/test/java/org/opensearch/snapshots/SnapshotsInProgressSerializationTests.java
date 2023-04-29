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

package org.opensearch.snapshots;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.SnapshotsInProgress.Entry;
import org.opensearch.cluster.SnapshotsInProgress.ShardState;
import org.opensearch.cluster.SnapshotsInProgress.State;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.Index;
import org.opensearch.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.test.AbstractDiffableWireSerializationTestCase;
import org.opensearch.test.VersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SnapshotsInProgressSerializationTests extends AbstractDiffableWireSerializationTestCase<Custom> {

    @Override
    protected Custom createTestInstance() {
        int numberOfSnapshots = randomInt(10);
        List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < numberOfSnapshots; i++) {
            entries.add(randomSnapshot());
        }
        return SnapshotsInProgress.of(entries);
    }

    private Entry randomSnapshot() {
        Snapshot snapshot = new Snapshot(randomAlphaOfLength(10), new SnapshotId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        boolean includeGlobalState = randomBoolean();
        boolean partial = randomBoolean();
        int numberOfIndices = randomIntBetween(0, 10);
        List<IndexId> indices = new ArrayList<>();
        for (int i = 0; i < numberOfIndices; i++) {
            indices.add(new IndexId(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        long startTime = randomLong();
        long repositoryStateId = randomLong();
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = new HashMap<>();
        final List<Index> esIndices = indices.stream()
            .map(i -> new Index(i.getName(), randomAlphaOfLength(10)))
            .collect(Collectors.toList());
        List<String> dataStreams = Arrays.asList(generateRandomStringArray(10, 10, false));
        for (Index idx : esIndices) {
            int shardsCount = randomIntBetween(1, 10);
            for (int j = 0; j < shardsCount; j++) {
                ShardId shardId = new ShardId(idx, j);
                String nodeId = randomAlphaOfLength(10);
                ShardState shardState = randomFrom(ShardState.values());
                builder.put(
                    shardId,
                    shardState == ShardState.QUEUED
                        ? SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
                        : new SnapshotsInProgress.ShardSnapshotStatus(
                            nodeId,
                            shardState,
                            shardState.failed() ? randomAlphaOfLength(10) : null,
                            "1"
                        )
                );
            }
        }
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = Collections.unmodifiableMap(builder);
        return new Entry(
            snapshot,
            includeGlobalState,
            partial,
            randomState(shards),
            indices,
            dataStreams,
            startTime,
            repositoryStateId,
            shards,
            null,
            SnapshotInfoTests.randomUserMetadata(),
            VersionUtils.randomVersion(random())
        );
    }

    @Override
    protected Writeable.Reader<Custom> instanceReader() {
        return SnapshotsInProgress::new;
    }

    @Override
    protected Custom makeTestChanges(Custom testInstance) {
        SnapshotsInProgress snapshots = (SnapshotsInProgress) testInstance;
        List<Entry> entries = new ArrayList<>(snapshots.entries());
        if (randomBoolean() && entries.size() > 1) {
            // remove some elements
            int leaveElements = randomIntBetween(0, entries.size() - 1);
            entries = randomSubsetOf(leaveElements, entries.toArray(new Entry[leaveElements]));
        }
        if (randomBoolean()) {
            // add some elements
            int addElements = randomInt(10);
            for (int i = 0; i < addElements; i++) {
                entries.add(randomSnapshot());
            }
        }
        if (randomBoolean()) {
            // modify some elements
            for (int i = 0; i < entries.size(); i++) {
                if (randomBoolean()) {
                    final Entry entry = entries.get(i);
                    entries.set(i, entry.fail(entry.shards(), randomState(entry.shards()), entry.failure()));
                }
            }
        }
        return SnapshotsInProgress.of(entries);
    }

    @Override
    protected Writeable.Reader<Diff<Custom>> diffReader() {
        return SnapshotsInProgress::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Custom mutateInstance(Custom instance) {
        List<Entry> entries = new ArrayList<>(((SnapshotsInProgress) instance).entries());
        boolean addEntry = entries.isEmpty() ? true : randomBoolean();
        if (addEntry) {
            entries.add(randomSnapshot());
        } else {
            entries.remove(randomIntBetween(0, entries.size() - 1));
        }
        return SnapshotsInProgress.of(entries);
    }

    public static State randomState(final Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.completed(shards.values())
            ? randomFrom(State.SUCCESS, State.FAILED)
            : randomFrom(State.STARTED, State.INIT, State.ABORTED);
    }
}
