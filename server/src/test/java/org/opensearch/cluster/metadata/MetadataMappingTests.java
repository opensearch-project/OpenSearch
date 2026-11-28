/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class MetadataMappingTests extends OpenSearchTestCase {

    private static final String MAPPING_JSON = """
        {
          "_doc": {
            "properties": {
              "title": { "type": "keyword" },
              "year":  { "type": "integer" }
            }
          }
        }
        """;

    private static final String OTHER_MAPPING_JSON = """
        {
          "_doc": {
            "properties": {
              "score": { "type": "float" }
            }
          }
        }
        """;

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());

    private static IndexMetadata.Builder newIndex(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }

    private static IndexMetadata newIndexWithMapping(String name, String mappingJson) throws IOException {
        return newIndex(name).putMapping(new MappingMetadata(new CompressedXContent(mappingJson))).build();
    }

    private static Metadata applyDiff(Metadata previous, Metadata current) throws IOException {
        return copyWriteable(current.diff(previous), REGISTRY, Metadata::readDiffFrom).apply(previous);
    }

    public void testDeduplicatesIdenticalMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        assertSame(metadata.index("index-1").mapping(), metadata.index("index-2").mapping());
    }

    public void testDoesNotDeduplicateDifferentMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", OTHER_MAPPING_JSON), false)
            .build();

        assertNotSame(metadata.index("index-1").mapping(), metadata.index("index-2").mapping());
    }

    public void testDeduplicatesIndicesPutAsBuilderOrWithVersionIncrement() throws Exception {
        IndexMetadata index2 = newIndexWithMapping("index-2", MAPPING_JSON);
        IndexMetadata index3 = newIndexWithMapping("index-3", MAPPING_JSON);

        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(IndexMetadata.builder(index2))
            .put(index3, true)
            .build();

        MappingMetadata sharedMapping = metadata.index("index-1").mapping();
        assertSame(sharedMapping, metadata.index("index-2").mapping());
        assertSame(sharedMapping, metadata.index("index-3").mapping());
        assertEquals(index2.getVersion() + 1, metadata.index("index-2").getVersion());
        assertEquals(index3.getVersion() + 1, metadata.index("index-3").getVersion());
    }

    public void testPuttingIndexBuilderUsesPooledMappingBeforeBuild() throws Exception {
        IndexMetadata.Builder index2 = newIndex("index-2").putMapping(new MappingMetadata(new CompressedXContent(MAPPING_JSON)));

        Metadata metadata = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false).put(index2).build();

        MappingMetadata sharedMapping = metadata.index("index-1").mapping();
        assertSame(sharedMapping, index2.mapping());
        assertSame(sharedMapping, metadata.index("index-2").mapping());
    }

    public void testDoesNotRebuildDeduplicatedIndices() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        Metadata rebuiltMetadata = Metadata.builder(metadata).build();

        assertSame(metadata.index("index-1"), rebuiltMetadata.index("index-1"));
        assertSame(metadata.index("index-2"), rebuiltMetadata.index("index-2"));
    }

    public void testDeduplicationPreservesIndexMetadata() throws Exception {
        IndexMetadata index1 = newIndexWithMapping("index-1", MAPPING_JSON);
        IndexMetadata index2 = IndexMetadata.builder("index-2")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.minimumIndexCompatibilityVersion().id)
                    .put(IndexMetadata.SETTING_VERSION_UPGRADED, Version.CURRENT.id)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, 2)
                    .put(IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), "2")
                    .put(IndexMetadata.SETTING_INDEX_APPEND_ONLY_ENABLED, true)
                    .put(IndexMetadata.INDEX_BULK_ADAPTIVE_SHARD_SELECTION_ENABLED.getKey(), true)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 3)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 5)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_REMOTE_CAPABLE_SHARDS_PER_NODE_SETTING.getKey(), 6)
                    .put(ShardsLimitAllocationDecider.INDEX_TOTAL_REMOTE_CAPABLE_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 7)
                    .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", "node-a")
                    .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", "node-b")
                    .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", "node-c")
                    .put(IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getKey() + "_id", "node-d")
            )
            .putMapping(new MappingMetadata(new CompressedXContent(MAPPING_JSON)))
            .state(IndexMetadata.State.CLOSE)
            .system(true)
            .putAlias(AliasMetadata.builder("alias-2"))
            .putCustom("custom", Map.of("key", "value"))
            .putInSyncAllocationIds(0, Set.of("allocation-0"))
            .primaryTerm(0, 19)
            .version(7)
            .mappingVersion(11)
            .settingsVersion(13)
            .aliasesVersion(17)
            .build();

        Metadata metadata = Metadata.builder().put(index1, false).put(index2, false).build();
        IndexMetadata deduplicated = metadata.index("index-2");

        assertNotSame(index2, deduplicated);
        assertSame(metadata.index("index-1").mapping(), deduplicated.mapping());
        assertEquals(index1, metadata.index("index-1"));
        assertEquals(index2, deduplicated);
        assertEquals(7, deduplicated.getVersion());
        assertEquals(11, deduplicated.getMappingVersion());
        assertEquals(13, deduplicated.getSettingsVersion());
        assertEquals(17, deduplicated.getAliasesVersion());
        assertEquals(4, deduplicated.getNumberOfShards());
        assertEquals(1, deduplicated.getNumberOfReplicas());
        assertEquals(2, deduplicated.getRoutingPartitionSize());
        assertEquals(index2.getWaitForActiveShards(), deduplicated.getWaitForActiveShards());
        assertTrue(deduplicated.isAppendOnlyIndex());
        assertTrue(deduplicated.bulkAdaptiveShardSelectionEnabled());
        assertEquals(index2.isRemoteSnapshot(), deduplicated.isRemoteSnapshot());
        assertEquals(index2.getTotalNumberOfShards(), deduplicated.getTotalNumberOfShards());
        assertEquals(index2.getRoutingNumShards(), deduplicated.getRoutingNumShards());
        assertEquals(index2.getRoutingFactor(), deduplicated.getRoutingFactor());
        assertEquals(3, deduplicated.getIndexTotalShardsPerNodeLimit());
        assertEquals(5, deduplicated.getIndexTotalPrimaryShardsPerNodeLimit());
        assertEquals(6, deduplicated.getIndexTotalRemoteCapableShardsPerNodeLimit());
        assertEquals(7, deduplicated.getIndexTotalRemoteCapablePrimaryShardsPerNodeLimit());
        assertEquals(Version.CURRENT.minimumIndexCompatibilityVersion(), deduplicated.getCreationVersion());
        assertEquals(Version.CURRENT, deduplicated.getUpgradedVersion());
        assertSame(index2.requireFilters(), deduplicated.requireFilters());
        assertSame(index2.includeFilters(), deduplicated.includeFilters());
        assertSame(index2.excludeFilters(), deduplicated.excludeFilters());
        assertSame(index2.getInitialRecoveryFilters(), deduplicated.getInitialRecoveryFilters());
    }

    public void testDeletingIndexRetainsSharedMapping() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        Metadata afterDeletion = Metadata.builder(metadata).remove("index-1").build();
        afterDeletion = Metadata.builder(afterDeletion).put(newIndexWithMapping("index-3", MAPPING_JSON), false).build();

        assertNull(afterDeletion.index("index-1"));
        assertSame(metadata.index("index-2").mapping(), afterDeletion.index("index-3").mapping());
    }

    public void testReadDeduplicatesMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        Metadata deserializedMetadata = copyWriteable(metadata, REGISTRY, Metadata::readFrom);

        assertSame(deserializedMetadata.index("index-1").mapping(), deserializedMetadata.index("index-2").mapping());
    }

    public void testReadingIndexMetadataUsesDeduplicatedMapping() throws Exception {
        IndexMetadata index = newIndexWithMapping("index-1", MAPPING_JSON);
        MappingMetadata sharedMapping = new MappingMetadata(new CompressedXContent(MAPPING_JSON));

        IndexMetadata read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            index.writeTo(out);
            read = IndexMetadata.readFrom(out.bytes().streamInput(), mapping -> {
                assertEquals(sharedMapping, mapping);
                return sharedMapping;
            });
        }

        assertSame(sharedMapping, read.mapping());
        assertEquals(index, read);
    }

    public void testParsingXContentDeduplicatesMappings() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();

        ToXContent.Params params = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject("meta-data").startObject("indices");
        for (IndexMetadata indexMetadata : metadata) {
            IndexMetadata.Builder.toXContent(indexMetadata, builder, params);
        }
        builder.endObject().endObject().endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            Metadata parsedMetadata = Metadata.fromXContent(parser);
            assertSame(parsedMetadata.index("index-1").mapping(), parsedMetadata.index("index-2").mapping());
        }
    }

    public void testApplyingDiffKeepsUnchangedIndexMetadataInstances() throws Exception {
        Metadata.Builder builder = Metadata.builder();
        for (int i = 0; i < 10; i++) {
            builder.put(newIndexWithMapping("index-" + i, MAPPING_JSON), false);
        }
        Metadata previous = builder.build();
        Metadata current = Metadata.builder(previous).put(newIndexWithMapping("new-index", MAPPING_JSON), false).build();

        Metadata applied = applyDiff(previous, current);

        for (int i = 0; i < 10; i++) {
            assertSame(previous.index("index-" + i), applied.index("index-" + i));
        }
        assertSame(previous.index("index-0").mapping(), applied.index("new-index").mapping());
    }

    public void testApplyingDiffReleasesMappingOfDeletedIndex() throws Exception {
        Metadata previous = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", OTHER_MAPPING_JSON), false)
            .build();
        MappingMetadata releasedMapping = previous.index("index-2").mapping();
        Metadata current = Metadata.builder(previous).remove("index-2").build();

        Metadata applied = applyDiff(previous, current);
        Metadata afterCreation = Metadata.builder(applied).put(newIndexWithMapping("index-3", OTHER_MAPPING_JSON), false).build();

        assertSame(previous.index("index-1"), applied.index("index-1"));
        assertNull(applied.index("index-2"));
        assertNotSame(releasedMapping, afterCreation.index("index-3").mapping());
    }

    public void testDeletingLastIndexReleasesMapping() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", OTHER_MAPPING_JSON), false)
            .build();
        MappingMetadata releasedMapping = metadata.index("index-2").mapping();

        metadata = Metadata.builder(metadata).remove("index-2").build();
        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-3", OTHER_MAPPING_JSON), false).build();

        assertNotSame(releasedMapping, metadata.index("index-3").mapping());
    }

    public void testChangingMappingReleasesPreviousMapping() throws Exception {
        Metadata metadata = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false).build();
        MappingMetadata releasedMapping = metadata.index("index-1").mapping();

        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-1", OTHER_MAPPING_JSON), false).build();
        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-2", MAPPING_JSON), false).build();

        assertNotSame(releasedMapping, metadata.index("index-2").mapping());
    }

    public void testChangingMappingRetainsMappingUsedByOtherIndex() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndexWithMapping("index-1", MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();
        MappingMetadata sharedMapping = metadata.index("index-1").mapping();

        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-1", OTHER_MAPPING_JSON), false).build();
        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-3", MAPPING_JSON), false).build();

        assertSame(sharedMapping, metadata.index("index-2").mapping());
        assertSame(sharedMapping, metadata.index("index-3").mapping());
    }

    public void testReplacedMappingReusedInSameBuildIsRetained() throws Exception {
        Metadata metadata = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false).build();
        MappingMetadata mapping = metadata.index("index-1").mapping();

        metadata = Metadata.builder(metadata)
            .put(newIndexWithMapping("index-1", OTHER_MAPPING_JSON), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();
        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-3", MAPPING_JSON), false).build();

        assertSame(mapping, metadata.index("index-2").mapping());
        assertSame(mapping, metadata.index("index-3").mapping());
    }

    public void testUnchangedMappingRetainsSharedInstance() throws Exception {
        Metadata metadata = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false).build();
        MappingMetadata sharedMapping = metadata.index("index-1").mapping();

        metadata = Metadata.builder(metadata).updateNumberOfReplicas(1, new String[] { "index-1" }).build();
        metadata = Metadata.builder(metadata).put(newIndexWithMapping("index-2", MAPPING_JSON), false).build();

        assertSame(sharedMapping, metadata.index("index-1").mapping());
        assertSame(sharedMapping, metadata.index("index-2").mapping());
    }

    public void testReusingBuilderDoesNotChangeBuiltMetadata() throws Exception {
        Metadata.Builder builder = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false);
        Metadata first = builder.build();
        Metadata second = builder.put(newIndexWithMapping("index-2", OTHER_MAPPING_JSON), false).build();

        Metadata fromFirst = Metadata.builder(first).put(newIndexWithMapping("index-3", OTHER_MAPPING_JSON), false).build();

        assertNotSame(second.index("index-2").mapping(), fromFirst.index("index-3").mapping());
    }

    public void testRemovingAllIndicesReleasesMappings() throws Exception {
        Metadata metadata = Metadata.builder().put(newIndexWithMapping("index-1", MAPPING_JSON), false).build();
        MappingMetadata releasedMapping = metadata.index("index-1").mapping();

        metadata = Metadata.builder(metadata).removeAllIndices().put(newIndexWithMapping("index-2", MAPPING_JSON), false).build();

        assertNull(metadata.index("index-1"));
        assertNotSame(releasedMapping, metadata.index("index-2").mapping());
    }

    public void testPurgeSkipsIndexWithoutMapping() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(newIndex("index-1").build(), false)
            .put(newIndexWithMapping("index-2", MAPPING_JSON), false)
            .build();
        metadata = Metadata.builder(metadata).remove("index-2").build();

        assertNull(metadata.index("index-1").mapping());
        assertNull(metadata.index("index-2"));
    }

    public void testEqualMappingsHaveEqualHashCodes() throws Exception {
        MappingMetadata mapping = new MappingMetadata(new CompressedXContent(MAPPING_JSON));
        MappingMetadata equalMapping = new MappingMetadata(new CompressedXContent(MAPPING_JSON));

        assertEquals(mapping, equalMapping);
        assertEquals(mapping.hashCode(), equalMapping.hashCode());
    }
}
