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

import org.opensearch.Version;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SnapshotInfoTests extends AbstractWireSerializingTestCase<SnapshotInfo> {

    /**
     * A snapshot whose {@code version_id} refers to an unsupported version (for example, an index
     * originally created on a legacy Elasticsearch version) must still be readable so that listing
     * snapshots and retrieving snapshot status do not fail for the whole repository. The version is
     * reported as {@code null} in that case rather than throwing.
     */
    public void testFromXContentInternalToleratesUnsupportedVersionId() throws IOException {
        // 7100299 is the legacy Elasticsearch 7.10.2 version id; it lacks the OpenSearch mask bit
        // and is therefore not supported by OpenSearch 3.x.
        final String json = "{\"snapshot\":{"
            + "\"name\":\"snap-legacy\","
            + "\"uuid\":\"abc123\","
            + "\"version_id\":7100299,"
            + "\"state\":\"SUCCESS\","
            + "\"indices\":[\"idx-1\"],"
            + "\"total_shards\":1,"
            + "\"successful_shards\":1"
            + "}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            SnapshotInfo snapshotInfo = SnapshotInfo.fromXContentInternal(parser);
            assertEquals("snap-legacy", snapshotInfo.snapshotId().getName());
            assertNull("unsupported version id must resolve to null instead of throwing", snapshotInfo.version());
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        }
    }

    /**
     * A supported {@code version_id} must continue to resolve to the corresponding {@link Version}.
     */
    public void testFromXContentInternalResolvesSupportedVersionId() throws IOException {
        final String json = "{\"snapshot\":{"
            + "\"name\":\"snap-ok\","
            + "\"uuid\":\"def456\","
            + "\"version_id\":"
            + Version.CURRENT.id
            + ","
            + "\"state\":\"SUCCESS\","
            + "\"indices\":[\"idx-1\"],"
            + "\"total_shards\":1,"
            + "\"successful_shards\":1"
            + "}}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            SnapshotInfo snapshotInfo = SnapshotInfo.fromXContentInternal(parser);
            assertEquals(Version.CURRENT, snapshotInfo.version());
        }
    }

    /**
     * A {@link SnapshotInfo} with an unknown (null) version, as produced for a snapshot created on an
     * unsupported version, must survive a transport-layer serialization round trip. This guards the
     * coordinating-node to client path used by snapshot listing and status APIs.
     */
    public void testWireRoundTripWithUnknownVersion() throws IOException {
        final String json = "{\"snapshot\":{"
            + "\"name\":\"snap-legacy\","
            + "\"uuid\":\"abc123\","
            + "\"version_id\":7100299,"
            + "\"state\":\"SUCCESS\","
            + "\"indices\":[\"idx-1\"],"
            + "\"total_shards\":1,"
            + "\"successful_shards\":1"
            + "}}";
        final SnapshotInfo original;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            original = SnapshotInfo.fromXContentInternal(parser);
        }
        assertNull(original.version());
        SnapshotInfo roundTripped = copyInstance(original);
        assertNull("null version must be preserved across the wire", roundTripped.version());
        assertEquals(original, roundTripped);
    }

    @Override
    protected SnapshotInfo createTestInstance() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        List<String> indices = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        List<String> dataStreams = Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)));

        String reason = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 15);

        long startTime = randomNonNegativeLong();
        long endTime = randomNonNegativeLong();

        int totalShards = randomIntBetween(0, 100);
        int failedShards = randomIntBetween(0, totalShards);

        List<SnapshotShardFailure> shardFailures = Arrays.asList(
            randomArray(failedShards, failedShards, SnapshotShardFailure[]::new, () -> {
                String indexName = randomAlphaOfLengthBetween(3, 50);
                int id = randomInt();
                ShardId shardId = ShardId.fromString("[" + indexName + "][" + id + "]");

                return new SnapshotShardFailure(randomAlphaOfLengthBetween(5, 10), shardId, randomAlphaOfLengthBetween(5, 10));
            })
        );

        Boolean includeGlobalState = randomBoolean() ? null : randomBoolean();

        Map<String, Object> userMetadata = randomUserMetadata();

        Boolean remoteStoreIndexShallowCopy = randomBoolean() ? null : randomBoolean();

        return new SnapshotInfo(
            snapshotId,
            indices,
            dataStreams,
            startTime,
            reason,
            endTime,
            totalShards,
            shardFailures,
            includeGlobalState,
            userMetadata,
            remoteStoreIndexShallowCopy,
            0
        );
    }

    @Override
    protected Writeable.Reader<SnapshotInfo> instanceReader() {
        return SnapshotInfo::new;
    }

    @Override
    protected SnapshotInfo mutateInstance(SnapshotInfo instance) {
        switch (randomIntBetween(0, 9)) {
            case 0:
                SnapshotId snapshotId = new SnapshotId(
                    randomValueOtherThan(instance.snapshotId().getName(), () -> randomAlphaOfLength(5)),
                    randomValueOtherThan(instance.snapshotId().getUUID(), () -> randomAlphaOfLength(5))
                );
                return new SnapshotInfo(
                    snapshotId,
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 1:
                int indicesSize = randomValueOtherThan(instance.indices().size(), () -> randomIntBetween(1, 10));
                List<String> indices = Arrays.asList(
                    randomArray(indicesSize, indicesSize, String[]::new, () -> randomAlphaOfLengthBetween(2, 20))
                );
                return new SnapshotInfo(
                    instance.snapshotId(),
                    indices,
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 2:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    randomValueOtherThan(instance.startTime(), OpenSearchTestCase::randomNonNegativeLong),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 3:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    randomValueOtherThan(instance.reason(), () -> randomAlphaOfLengthBetween(5, 15)),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 4:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    randomValueOtherThan(instance.endTime(), OpenSearchTestCase::randomNonNegativeLong),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 5:
                int totalShards = randomValueOtherThan(instance.totalShards(), () -> randomIntBetween(0, 100));
                int failedShards = randomIntBetween(0, totalShards);

                List<SnapshotShardFailure> shardFailures = Arrays.asList(
                    randomArray(failedShards, failedShards, SnapshotShardFailure[]::new, () -> {
                        String indexName = randomAlphaOfLengthBetween(3, 50);
                        int id = randomInt();
                        ShardId shardId = ShardId.fromString("[" + indexName + "][" + id + "]");

                        return new SnapshotShardFailure(randomAlphaOfLengthBetween(5, 10), shardId, randomAlphaOfLengthBetween(5, 10));
                    })
                );
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    totalShards,
                    shardFailures,
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 6:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    Boolean.FALSE.equals(instance.includeGlobalState()),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 7:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    randomValueOtherThan(instance.userMetadata(), SnapshotInfoTests::randomUserMetadata),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    0
                );
            case 8:
                List<String> dataStreams = randomValueOtherThan(
                    instance.dataStreams(),
                    () -> Arrays.asList(randomArray(1, 10, String[]::new, () -> randomAlphaOfLengthBetween(2, 20)))
                );
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    dataStreams,
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    instance.isRemoteStoreIndexShallowCopyEnabled(),
                    123456
                );
            case 9:
                return new SnapshotInfo(
                    instance.snapshotId(),
                    instance.indices(),
                    instance.dataStreams(),
                    instance.startTime(),
                    instance.reason(),
                    instance.endTime(),
                    instance.totalShards(),
                    instance.shardFailures(),
                    instance.includeGlobalState(),
                    instance.userMetadata(),
                    Boolean.FALSE.equals(instance.isRemoteStoreIndexShallowCopyEnabled()),
                    123456
                );
            default:
                throw new IllegalArgumentException("invalid randomization case");
        }
    }

    public static Map<String, Object> randomUserMetadata() {
        if (randomBoolean()) {
            return null;
        }

        Map<String, Object> metadata = new HashMap<>();
        long fields = randomLongBetween(0, 25);
        for (int i = 0; i < fields; i++) {
            if (randomBoolean()) {
                metadata.put(
                    randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                    randomAlphaOfLengthBetween(5, 15)
                );
            } else {
                Map<String, Object> nested = new HashMap<>();
                long nestedFields = randomLongBetween(0, 25);
                for (int j = 0; j < nestedFields; j++) {
                    nested.put(
                        randomValueOtherThanMany(nested::containsKey, () -> randomAlphaOfLengthBetween(2, 10)),
                        randomAlphaOfLengthBetween(5, 15)
                    );
                }
                metadata.put(randomValueOtherThanMany(metadata::containsKey, () -> randomAlphaOfLengthBetween(2, 10)), nested);
            }
        }
        return metadata;
    }
}
