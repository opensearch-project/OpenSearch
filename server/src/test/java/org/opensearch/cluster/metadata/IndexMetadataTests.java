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

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.rollover.MaxAgeCondition;
import org.opensearch.action.admin.indices.rollover.MaxDocsCondition;
import org.opensearch.action.admin.indices.rollover.MaxSizeCondition;
import org.opensearch.action.admin.indices.rollover.RolloverInfo;
import org.opensearch.cluster.Diff;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.metadata.index.model.IndexMetadataModel;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.Version.MASK;
import static org.opensearch.cluster.metadata.IndexMetadata.parseIndexNameCounter;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexMetadataTests extends OpenSearchTestCase {

    private IndicesModule INDICES_MODULE = new IndicesModule(Collections.emptyList());

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(INDICES_MODULE.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(IndicesModule.getNamedXContents());
    }

    // Create the index metadata for a given index, with the specified version.
    private static IndexMetadata createIndexMetadata(final Index index, final long version) {
        return createIndexMetadata(index, version, false);
    }

    private static IndexMetadata createIndexMetadata(final Index index, final long version, final boolean isSystem) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return IndexMetadata.builder(index.getName())
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .creationDate(System.currentTimeMillis())
            .version(version)
            .system(isSystem)
            .build();
    }

    public void testIndexMetadataSerialization() throws IOException {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int numberOfReplicas = randomIntBetween(0, 10);
        final boolean system = randomBoolean();
        Map<String, String> customMap = new HashMap<>();
        customMap.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        customMap.put(randomAlphaOfLength(10), randomAlphaOfLength(15));
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", numShard)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .primaryTerm(0, 2)
            .setRoutingNumShards(32)
            .system(system)
            .putCustom("my_custom", customMap)
            .putRolloverInfo(
                new RolloverInfo(
                    randomAlphaOfLength(5),
                    Arrays.asList(
                        new MaxAgeCondition(TimeValue.timeValueMillis(randomNonNegativeLong())),
                        new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())),
                        new MaxDocsCondition(randomNonNegativeLong())
                    ),
                    randomNonNegativeLong()
                )
            )
            .context(new Context(randomAlphaOfLength(5)))
            .build();
        assertEquals(system, metadata.isSystem());

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        IndexMetadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        final IndexMetadata fromXContentMeta = IndexMetadata.fromXContent(parser);
        assertEquals(
            "expected: "
                + Strings.toString(MediaTypeRegistry.JSON, metadata)
                + "\nactual  : "
                + Strings.toString(MediaTypeRegistry.JSON, fromXContentMeta),
            metadata,
            fromXContentMeta
        );
        assertEquals(metadata.hashCode(), fromXContentMeta.hashCode());

        assertEquals(metadata.getNumberOfReplicas(), fromXContentMeta.getNumberOfReplicas());
        assertEquals(metadata.getNumberOfShards(), fromXContentMeta.getNumberOfShards());
        assertEquals(metadata.getCreationVersion(), fromXContentMeta.getCreationVersion());
        assertEquals(metadata.getRoutingNumShards(), fromXContentMeta.getRoutingNumShards());
        assertEquals(metadata.getCreationDate(), fromXContentMeta.getCreationDate());
        assertEquals(metadata.getRoutingFactor(), fromXContentMeta.getRoutingFactor());
        assertEquals(metadata.primaryTerm(0), fromXContentMeta.primaryTerm(0));
        assertEquals(metadata.isSystem(), fromXContentMeta.isSystem());
        assertEquals(metadata.context(), fromXContentMeta.context());
        final Map<String, DiffableStringMap> expectedCustom = Map.of("my_custom", new DiffableStringMap(customMap));
        assertEquals(metadata.getCustomData(), expectedCustom);
        assertEquals(metadata.getCustomData(), fromXContentMeta.getCustomData());

        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
            IndexMetadata deserialized = IndexMetadata.readFrom(in);
            assertEquals(metadata, deserialized);
            assertEquals(metadata.hashCode(), deserialized.hashCode());

            assertEquals(metadata.getNumberOfReplicas(), deserialized.getNumberOfReplicas());
            assertEquals(metadata.getNumberOfShards(), deserialized.getNumberOfShards());
            assertEquals(metadata.getCreationVersion(), deserialized.getCreationVersion());
            assertEquals(metadata.getRoutingNumShards(), deserialized.getRoutingNumShards());
            assertEquals(metadata.getCreationDate(), deserialized.getCreationDate());
            assertEquals(metadata.getRoutingFactor(), deserialized.getRoutingFactor());
            assertEquals(metadata.primaryTerm(0), deserialized.primaryTerm(0));
            assertEquals(metadata.getRolloverInfos(), deserialized.getRolloverInfos());
            assertEquals(deserialized.getCustomData(), expectedCustom);
            assertEquals(metadata.getCustomData(), deserialized.getCustomData());
            assertEquals(metadata.isSystem(), deserialized.isSystem());
            assertEquals(metadata.context(), deserialized.context());
        }
    }

    public void testWriteVerifiableTo() throws IOException {
        int numberOfReplicas = randomIntBetween(0, 10);
        final boolean system = randomBoolean();
        Map<String, String> customMap = new HashMap<>();
        customMap.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        customMap.put(randomAlphaOfLength(10), randomAlphaOfLength(15));

        RolloverInfo info1 = new RolloverInfo(
            randomAlphaOfLength(5),
            Arrays.asList(
                new MaxAgeCondition(TimeValue.timeValueMillis(randomNonNegativeLong())),
                new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())),
                new MaxDocsCondition(randomNonNegativeLong())
            ),
            randomNonNegativeLong()
        );
        RolloverInfo info2 = new RolloverInfo(
            randomAlphaOfLength(5),
            Arrays.asList(
                new MaxAgeCondition(TimeValue.timeValueMillis(randomNonNegativeLong())),
                new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())),
                new MaxDocsCondition(randomNonNegativeLong())
            ),
            randomNonNegativeLong()
        );
        String mappings = "    {\n"
            + "        \"_doc\": {\n"
            + "            \"properties\": {\n"
            + "                \"actiongroups\": {\n"
            + "                    \"type\": \"text\",\n"
            + "                    \"fields\": {\n"
            + "                        \"keyword\": {\n"
            + "                            \"type\": \"keyword\",\n"
            + "                            \"ignore_above\": 256\n"
            + "                        }\n"
            + "                    }\n"
            + "                },\n"
            + "                \"allowlist\": {\n"
            + "                    \"type\": \"text\",\n"
            + "                    \"fields\": {\n"
            + "                        \"keyword\": {\n"
            + "                            \"type\": \"keyword\",\n"
            + "                            \"ignore_above\": 256\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }";
        IndexMetadata metadata1 = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 4)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .primaryTerm(0, 2)
            .primaryTerm(1, 3)
            .setRoutingNumShards(32)
            .system(system)
            .putCustom("my_custom", customMap)
            .putCustom("my_custom2", customMap)
            .putAlias(AliasMetadata.builder("alias-1").routing("routing-1").build())
            .putAlias(AliasMetadata.builder("alias-2").routing("routing-2").build())
            .putRolloverInfo(info1)
            .putRolloverInfo(info2)
            .putInSyncAllocationIds(0, Set.of("1", "2", "3"))
            .putMapping(mappings)
            .build();

        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        metadata1.writeVerifiableTo(checksumOut);
        assertNotNull(metadata1.toString());

        IndexMetadata metadata2 = IndexMetadata.builder(metadata1.getIndex().getName())
            .settings(
                Settings.builder()
                    .put("index.number_of_replicas", numberOfReplicas)
                    .put("index.number_of_shards", 4)
                    .put("index.version.created", 1 ^ MASK)
                    .build()
            )
            .creationDate(metadata1.getCreationDate())
            .primaryTerm(1, 3)
            .primaryTerm(0, 2)
            .setRoutingNumShards(32)
            .system(system)
            .putCustom("my_custom2", customMap)
            .putCustom("my_custom", customMap)
            .putAlias(AliasMetadata.builder("alias-2").routing("routing-2").build())
            .putAlias(AliasMetadata.builder("alias-1").routing("routing-1").build())
            .putRolloverInfo(info2)
            .putRolloverInfo(info1)
            .putInSyncAllocationIds(0, Set.of("3", "1", "2"))
            .putMapping(mappings)
            .build();

        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        metadata2.writeVerifiableTo(checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }

    public void testGetRoutingFactor() {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int routingFactor = IndexMetadata.getRoutingFactor(32, numShard);
        assertEquals(routingFactor * numShard, 32);

        Integer brokenNumShards = randomFrom(3, 5, 9, 12, 29, 42);
        expectThrows(IllegalArgumentException.class, () -> IndexMetadata.getRoutingFactor(32, brokenNumShards));
    }

    public void testSelectShrinkShards() {
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 32)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .build();
        Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(0, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 0),
                new ShardId(metadata.getIndex(), 1),
                new ShardId(metadata.getIndex(), 2),
                new ShardId(metadata.getIndex(), 3)
            )
        );
        shardIds = IndexMetadata.selectShrinkShards(1, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 4),
                new ShardId(metadata.getIndex(), 5),
                new ShardId(metadata.getIndex(), 6),
                new ShardId(metadata.getIndex(), 7)
            )
        );
        shardIds = IndexMetadata.selectShrinkShards(7, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 28),
                new ShardId(metadata.getIndex(), 29),
                new ShardId(metadata.getIndex(), 30),
                new ShardId(metadata.getIndex(), 31)
            )
        );

        assertEquals(
            "the number of target shards (8) must be greater than the shard id: 8",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectShrinkShards(8, metadata, 8)).getMessage()
        );
    }

    public void testSelectCloneShard() {
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 10)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .build();

        assertEquals(
            "the number of target shards (11) must be the same as the number of source shards (10)",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectCloneShard(0, metadata, 11)).getMessage()
        );
    }

    public void testSelectResizeShards() {
        int numTargetShards = randomFrom(4, 6, 8, 12);

        IndexMetadata split = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .setRoutingNumShards(numTargetShards * 2)
            .build();

        IndexMetadata shrink = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 32)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .build();
        int shard = randomIntBetween(0, numTargetShards - 1);
        assertEquals(
            Collections.singleton(IndexMetadata.selectSplitShard(shard, split, numTargetShards)),
            IndexMetadata.selectRecoverFromShards(shard, split, numTargetShards)
        );

        numTargetShards = randomFrom(1, 2, 4, 8, 16);
        shard = randomIntBetween(0, numTargetShards - 1);
        assertEquals(
            IndexMetadata.selectShrinkShards(shard, shrink, numTargetShards),
            IndexMetadata.selectRecoverFromShards(shard, shrink, numTargetShards)
        );

        IndexMetadata.selectRecoverFromShards(0, shrink, 32);
    }

    public void testSelectSplitShard() {
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .setRoutingNumShards(4)
            .build();
        ShardId shardId = IndexMetadata.selectSplitShard(0, metadata, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(1, metadata, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(2, metadata, 4);
        assertEquals(1, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(3, metadata, 4);
        assertEquals(1, shardId.getId());

        assertEquals(
            "the number of target shards (0) must be greater than the shard id: 0",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 0)).getMessage()
        );

        assertEquals(
            "the number of source shards [2] must be a factor of [3]",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 3)).getMessage()
        );

        assertEquals(
            "the number of routing shards [4] must be a multiple of the target shards [8]",
            expectThrows(IllegalStateException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 8)).getMessage()
        );
    }

    public void testIndexFormat() {
        Settings defaultSettings = Settings.builder()
            .put("index.version.created", 1 ^ MASK)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .build();

        // matching version
        {
            IndexMetadata metadata = IndexMetadata.builder("foo")
                .settings(
                    Settings.builder()
                        .put(defaultSettings)
                        // intentionally not using the constant, so upgrading requires you to look at this test
                        // where you have to update this part and the next one
                        .put("index.format", 6)
                        .build()
                )
                .build();

            assertThat(metadata.getSettings().getAsInt(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 0), is(6));
        }

        // no setting configured
        {
            IndexMetadata metadata = IndexMetadata.builder("foo").settings(Settings.builder().put(defaultSettings).build()).build();
            assertThat(metadata.getSettings().getAsInt(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 0), is(0));
        }
    }

    public void testNumberOfRoutingShards() {
        Settings build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 10).build();
        assertEquals(10, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 5).build();
        assertEquals(5, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        int numShards = randomIntBetween(1, 10);
        build = Settings.builder().put("index.number_of_shards", numShards).build();
        assertEquals(numShards, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        Settings lessThanSettings = Settings.builder().put("index.number_of_shards", 8).put("index.number_of_routing_shards", 4).build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(lessThanSettings)
        );
        assertEquals("index.number_of_routing_shards [4] must be >= index.number_of_shards [8]", iae.getMessage());

        Settings notAFactorySettings = Settings.builder().put("index.number_of_shards", 2).put("index.number_of_routing_shards", 3).build();
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(notAFactorySettings)
        );
        assertEquals("the number of source shards [2] must be a factor of [3]", iae.getMessage());
    }

    public void testMissingNumberOfShards() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> IndexMetadata.builder("test").build());
        assertThat(e.getMessage(), containsString("must specify number of shards for index [test]"));
    }

    public void testNumberOfShardsIsNotZero() {
        runTestNumberOfShardsIsPositive(0);
    }

    public void testNumberOfShardsIsNotNegative() {
        runTestNumberOfShardsIsPositive(-randomIntBetween(1, Integer.MAX_VALUE));
    }

    private void runTestNumberOfShardsIsPositive(final int numberOfShards) {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse value [" + numberOfShards + "] for setting [index.number_of_shards] must be >= 1")
        );
    }

    public void testMissingNumberOfReplicas() {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8)).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(e.getMessage(), containsString("must specify number of replicas for index [test]"));
    }

    public void testNumberOfReplicasIsNonNegative() {
        final int numberOfReplicas = -randomIntBetween(1, Integer.MAX_VALUE);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse value [" + numberOfReplicas + "] for setting [index.number_of_replicas] must be >= 0")
        );
    }

    public void testParseIndexNameReturnsCounter() {
        assertThat(parseIndexNameCounter(".ds-logs-000003"), is(3));
        assertThat(parseIndexNameCounter("shrink-logs-000003"), is(3));
    }

    public void testParseIndexNameSupportsDateMathPattern() {
        assertThat(parseIndexNameCounter("<logs-{now/d}-1>"), is(1));
    }

    public void testParseIndexNameThrowExceptionWhenNoSeparatorIsPresent() {
        try {
            parseIndexNameCounter("testIndexNameWithoutDash");
            fail("expected to fail as the index name contains no - separator");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no - separator found in index name [testIndexNameWithoutDash]"));
        }
    }

    public void testParseIndexNameCannotFormatNumber() {
        try {
            parseIndexNameCounter("testIndexName-000a2");
            fail("expected to fail as the index name doesn't end with digits");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("unable to parse the index name [testIndexName-000a2] to extract the counter"));
        }
    }

    /**
     * Test that changes to indices metadata are applied
     */
    public void testIndicesMetadataDiffSystemFlagFlipped() {
        String indexUuid = UUIDs.randomBase64UUID();
        Index index = new Index("test-index", indexUuid);
        IndexMetadata previousIndexMetadata = createIndexMetadata(index, 1);
        IndexMetadata nextIndexMetadata = createIndexMetadata(index, 2, true);
        Diff<IndexMetadata> diff = new IndexMetadata.IndexMetadataDiff(previousIndexMetadata, nextIndexMetadata);
        IndexMetadata indexMetadataAfterDiffApplied = diff.apply(previousIndexMetadata);
        assertTrue(indexMetadataAfterDiffApplied.isSystem());
        assertThat(indexMetadataAfterDiffApplied.getVersion(), equalTo(nextIndexMetadata.getVersion()));
    }

    /**
     * Test validation for remote store segment path prefix setting
     */
    public void testRemoteStoreSegmentPathPrefixValidation() {
        // Test empty value (should be allowed)
        final Settings emptySettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "")
            .build();

        IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(emptySettings);

        final Settings whitespaceSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "   ")
            .build();

        IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(whitespaceSettings);

        final Settings validSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer-node-1")
            .build();

        String value = IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(validSettings);
        assertEquals("writer-node-1", value);

        final Settings disabledSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer-node-1")
            .build();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(disabledSettings);
        });
        assertTrue(e.getMessage().contains("can only be set when"));

        final Settings noRemoteStoreSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer-node-1")
            .build();

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(noRemoteStoreSettings); }
        );
        assertTrue(e.getMessage().contains("can only be set when"));

        final Settings invalidPathSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer/node")
            .build();

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(invalidPathSettings); }
        );
        assertTrue(e.getMessage().contains("cannot contain path separators"));

        final Settings backslashSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer\\node")
            .build();

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(backslashSettings); }
        );
        assertTrue(e.getMessage().contains("cannot contain path separators"));

        final Settings colonSettings = Settings.builder()
            .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.getKey(), "writer:node")
            .build();

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { IndexMetadata.INDEX_REMOTE_STORE_SEGMENT_PATH_PREFIX.get(colonSettings); }
        );
        assertTrue(e.getMessage().contains("cannot contain path separators"));
    }

    /**
     * Test validation for pull-based ingestion all-active settings.
     */
    public void testAllActivePullBasedIngestionSettings() {
        // all-active ingestion enabled with default (document) replication mode
        final Settings settings1 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), true)
            .put(IndexMetadata.INGESTION_SOURCE_TYPE_SETTING.getKey(), "kafka")
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
            .build();

        boolean isAllActiveIngestionEnabled = IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings1);
        assertTrue(isAllActiveIngestionEnabled);

        // all-active ingestion disabled in segment replication mode
        final Settings settings2 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), false)
            .put(IndexMetadata.INGESTION_SOURCE_TYPE_SETTING.getKey(), "kafka")
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        isAllActiveIngestionEnabled = IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings2);
        assertFalse(isAllActiveIngestionEnabled);

        // all-active ingestion disabled in document replication mode
        final Settings settings3 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), false)
            .put(IndexMetadata.INGESTION_SOURCE_TYPE_SETTING.getKey(), "kafka")
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
            .build();

        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> {
            IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings3);
        });
        assertTrue(e1.getMessage().contains("is not supported in pull-based ingestion"));

        // all-active ingestion enabled in segment replication mode
        final Settings settings4 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), true)
            .put(IndexMetadata.INGESTION_SOURCE_TYPE_SETTING.getKey(), "kafka")
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> {
            IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings4);
        });
        assertTrue(e2.getMessage().contains("is not supported in pull-based ingestion"));

        // all-active ingestion validations do not apply when pull-based ingestion is not enabled
        final Settings settings5 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), true)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .build();

        isAllActiveIngestionEnabled = IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings5);
        assertTrue(isAllActiveIngestionEnabled);

        // all-active ingestion validations do not apply when pull-based ingestion is not enabled
        final Settings settings6 = Settings.builder()
            .put(IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.getKey(), false)
            .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.DOCUMENT)
            .build();

        isAllActiveIngestionEnabled = IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING.get(settings6);
        assertFalse(isAllActiveIngestionEnabled);
    }

    public void testModelAccessor() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 1)
                    .build()
            )
            .creationDate(System.currentTimeMillis())
            .primaryTerm(0, 1)
            .primaryTerm(1, 2)
            .build();

        // Verify model() accessor returns the underlying model
        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertEquals("test-index", model.index());
        // Settings stores numeric values as strings
        assertEquals("2", model.settings().getSettings().get("index.number_of_shards"));
        assertArrayEquals(new long[] { 1, 2 }, model.primaryTerms());
    }

    public void testModelWithAliases() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .putAlias(AliasMetadata.builder("alias1").routing("routing1").build())
            .putAlias(AliasMetadata.builder("alias2").writeIndex(true).build())
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertEquals(2, model.aliases().size());
        assertNotNull(model.aliases().get("alias1"));
        assertNotNull(model.aliases().get("alias2"));
    }

    public void testModelWithMappings() throws IOException {
        String mappings = "{ \"_doc\": { \"properties\": { \"field1\": { \"type\": \"text\" } } } }";
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .putMapping(mappings)
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertFalse(model.mappings().isEmpty());
    }

    public void testModelWithContext() {
        Context context = new Context("test-context", "1.0", Map.of("key", "value"));
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .context(context)
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertNotNull(model.context());
        assertEquals("test-context", model.context().name());
        assertEquals("1.0", model.context().version());
    }

    public void testModelWithInSyncAllocationIds() {
        Set<String> allocationIds = Set.of("alloc-1", "alloc-2", "alloc-3");
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .putInSyncAllocationIds(0, allocationIds)
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertEquals(allocationIds, model.inSyncAllocationIds().get(0));
    }

    public void testModelPreservesVersionInfo() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .version(5)
            .mappingVersion(2)
            .settingsVersion(3)
            .aliasesVersion(4)
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertEquals(5, model.version());
        assertEquals(2, model.mappingVersion());
        assertEquals(3, model.settingsVersion());
        assertEquals(4, model.aliasesVersion());
    }

    public void testModelWithSystemIndex() {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .system(true)
            .build();

        IndexMetadataModel model = metadata.model();
        assertNotNull(model);
        assertTrue(model.isSystem());
    }

    public void testModelDeserializationWithMinimalValues() throws IOException {
        // Create IndexMetadata with minimal required fields only
        IndexMetadata metadata = IndexMetadata.builder("minimal-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .build();

        // Serialize using IndexMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // Deserialize using IndexMetadataModel with readers
        final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        final IndexMetadataModel model = new IndexMetadataModel(
            in,
            DiffableStringMap.METADATA_READER,
            RolloverInfo.METADATA_READER,
            rolloverInfo -> ((RolloverInfo) rolloverInfo).getAlias()
        );

        // Verify minimal fields
        assertEquals("minimal-index", model.index());
        assertEquals(1, model.version());
        assertEquals(1, model.mappingVersion());
        assertEquals(1, model.settingsVersion());
        assertEquals(1, model.aliasesVersion());
        assertFalse(model.isSystem());
        assertTrue(model.aliases().isEmpty());
        assertNull(model.context());
        assertTrue(model.rolloverInfos().isEmpty());
    }

    public void testModelDeserialization() throws IOException {
        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 1)
                    .build()
            )
            .creationDate(System.currentTimeMillis())
            .primaryTerm(0, 1)
            .primaryTerm(1, 2)
            .putAlias(AliasMetadata.builder("test-alias").routing("routing").build())
            .version(5)
            .system(true)
            .build();

        // Serialize using IndexMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // Deserialize using IndexMetadataModel with readers
        final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        final IndexMetadataModel model = new IndexMetadataModel(
            in,
            DiffableStringMap.METADATA_READER,
            RolloverInfo.METADATA_READER,
            rolloverInfo -> ((RolloverInfo) rolloverInfo).getAlias()
        );

        // Verify all fields match
        assertEquals(metadata.getIndex().getName(), model.index());
        assertEquals(metadata.getVersion(), model.version());
        assertEquals(metadata.getMappingVersion(), model.mappingVersion());
        assertEquals(metadata.getSettingsVersion(), model.settingsVersion());
        assertEquals(metadata.getAliasesVersion(), model.aliasesVersion());
        assertEquals(metadata.getRoutingNumShards(), model.routingNumShards());
        assertEquals(metadata.getState().id(), model.state());
        assertEquals(metadata.primaryTerm(0), model.primaryTerms()[0]);
        assertEquals(metadata.primaryTerm(1), model.primaryTerms()[1]);
        assertEquals(metadata.isSystem(), model.isSystem());
        assertEquals(metadata.getAliases().size(), model.aliases().size());
    }

    public void testModelDeserializationWithMappingsAndContext() throws IOException {
        String mappings = "{ \"_doc\": { \"properties\": { \"field1\": { \"type\": \"text\" } } } }";
        Context context = new Context("test-context", "1.0", Map.of("key", "value"));

        IndexMetadata metadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .putMapping(mappings)
            .context(context)
            .build();

        // Serialize using IndexMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // Deserialize using IndexMetadataModel with readers
        final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        final IndexMetadataModel model = new IndexMetadataModel(
            in,
            DiffableStringMap.METADATA_READER,
            RolloverInfo.METADATA_READER,
            rolloverInfo -> ((RolloverInfo) rolloverInfo).getAlias()
        );

        // Verify mappings and context
        assertFalse(model.mappings().isEmpty());
        assertNotNull(model.context());
        assertEquals("test-context", model.context().name());
    }

    public void testModelToIndexMetadataSerialization() throws IOException {
        IndexMetadata original = IndexMetadata.builder("roundtrip-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 1)
                    .build()
            )
            .creationDate(System.currentTimeMillis())
            .primaryTerm(0, 1)
            .primaryTerm(1, 2)
            .putAlias(AliasMetadata.builder("alias1").routing("routing1").build())
            .version(5)
            .mappingVersion(2)
            .settingsVersion(3)
            .aliasesVersion(4)
            .build();

        // Serialize IndexMetadata
        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as IndexMetadataModel
        final StreamInput in1 = new NamedWriteableAwareStreamInput(out1.bytes().streamInput(), writableRegistry());
        final IndexMetadataModel model = new IndexMetadataModel(
            in1,
            DiffableStringMap.METADATA_READER,
            RolloverInfo.METADATA_READER,
            rolloverInfo -> ((RolloverInfo) rolloverInfo).getAlias()
        );

        // Serialize the model
        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        // Deserialize as IndexMetadata
        final StreamInput in2 = new NamedWriteableAwareStreamInput(out2.bytes().streamInput(), writableRegistry());
        final IndexMetadata restored = IndexMetadata.readFrom(in2);

        // Verify round-trip preserves data
        assertEquals(original.getIndex().getName(), restored.getIndex().getName());
        assertEquals(original.getVersion(), restored.getVersion());
        assertEquals(original.getMappingVersion(), restored.getMappingVersion());
        assertEquals(original.getSettingsVersion(), restored.getSettingsVersion());
        assertEquals(original.getAliasesVersion(), restored.getAliasesVersion());
        assertEquals(original.getRoutingNumShards(), restored.getRoutingNumShards());
        assertEquals(original.getState(), restored.getState());
        assertEquals(original.primaryTerm(0), restored.primaryTerm(0));
        assertEquals(original.primaryTerm(1), restored.primaryTerm(1));
        assertEquals(original.getAliases().keySet(), restored.getAliases().keySet());
    }

    public void testModelRoundTripWithAllFields() throws IOException {
        Map<String, String> customMap = new HashMap<>();
        customMap.put("custom_key", "custom_value");

        IndexMetadata original = IndexMetadata.builder("full-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", 4)
                    .put("index.number_of_replicas", 2)
                    .build()
            )
            .creationDate(System.currentTimeMillis())
            .primaryTerm(0, 1)
            .primaryTerm(1, 2)
            .primaryTerm(2, 3)
            .primaryTerm(3, 4)
            .setRoutingNumShards(32)
            .putAlias(AliasMetadata.builder("alias1").routing("r1").writeIndex(true).build())
            .putAlias(AliasMetadata.builder("alias2").filter("{\"term\":{\"user\":\"test\"}}").build())
            .putCustom("my_custom", customMap)
            .putInSyncAllocationIds(0, Set.of("alloc-1", "alloc-2"))
            .putInSyncAllocationIds(1, Set.of("alloc-3"))
            .version(10)
            .mappingVersion(5)
            .settingsVersion(3)
            .aliasesVersion(2)
            .system(true)
            .context(new Context("ctx", "1.0", Map.of("param", "value")))
            .build();

        // Get model from original
        IndexMetadataModel model = original.model();

        // Serialize the model
        final BytesStreamOutput out = new BytesStreamOutput();
        model.writeTo(out);

        // Deserialize as IndexMetadata
        final StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        final IndexMetadata restored = IndexMetadata.readFrom(in);

        // Verify all fields
        assertEquals(original.getIndex().getName(), restored.getIndex().getName());
        assertEquals(original.getVersion(), restored.getVersion());
        assertEquals(original.getMappingVersion(), restored.getMappingVersion());
        assertEquals(original.getSettingsVersion(), restored.getSettingsVersion());
        assertEquals(original.getAliasesVersion(), restored.getAliasesVersion());
        assertEquals(original.getRoutingNumShards(), restored.getRoutingNumShards());
        assertEquals(original.getState(), restored.getState());
        assertEquals(original.primaryTerm(0), restored.primaryTerm(0));
        assertEquals(original.primaryTerm(1), restored.primaryTerm(1));
        assertEquals(original.primaryTerm(2), restored.primaryTerm(2));
        assertEquals(original.primaryTerm(3), restored.primaryTerm(3));
        assertEquals(original.isSystem(), restored.isSystem());
        assertEquals(original.getAliases().size(), restored.getAliases().size());
        assertEquals(original.getInSyncAllocationIds(), restored.getInSyncAllocationIds());
        assertEquals(original.getCustomData(), restored.getCustomData());
        assertEquals(original.context(), restored.context());
    }
}
