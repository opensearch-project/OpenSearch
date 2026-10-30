/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link ObjectStoreSourceConfigValidator}, covering the valid cases and one failing case per hard-reject rule.
 */
public class ObjectStoreSourceConfigValidatorTests extends OpenSearchTestCase {

    private static Map<String, Object> baseUrl() {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("format", "ndjson");
        params.put("shard_split", "equal_bytes");
        params.put("source_url", "dump/events.ndjson");
        params.put("manifest", "[{\"key\":\"dump/events.ndjson\",\"size\":100}]");
        return params;
    }

    private static Map<String, Object> basePrefix() {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("format", "ndjson");
        params.put("shard_split", "whole_files");
        params.put("source_prefix", "dump/");
        params.put("manifest", "[{\"key\":\"dump/part-0.ndjson\",\"size\":10},{\"key\":\"dump/part-1.ndjson\",\"size\":20}]");
        return params;
    }

    private static ObjectStoreSourceConfig config(Map<String, Object> params) {
        return new ObjectStoreSourceConfig(params);
    }

    public void testValidSingleObjectConfigPasses() {
        ObjectStoreSourceConfig config = config(baseUrl());
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getShardSplit(), is(ShardSplit.EQUAL_BYTES));
        assertThat(config.getDiscovery(), is(Discovery.MANIFEST));
        assertThat(config.getSourceUrl(), is("dump/events.ndjson"));
    }

    public void testValidPrefixWholeFilesConfigPasses() {
        ObjectStoreSourceConfig config = config(basePrefix());
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getShardSplit(), is(ShardSplit.WHOLE_FILES));
        assertThat(config.getDiscovery(), is(Discovery.MANIFEST));
        assertThat(config.getSourcePrefix(), is("dump/"));
    }

    public void testConsecutiveDotsInsideAFileNameAccepted() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/a..b.ndjson\",\"size\":10},{\"key\":\"dump/v1..2/part.ndjson\",\"size\":20}]");
        ObjectStoreSourceConfig config = config(params);
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getShardSplit(), is(ShardSplit.WHOLE_FILES));
    }

    public void testParentDirectorySegmentStillRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/../etc/passwd\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("path traversal"));
    }

    public void testCurrentDirectorySegmentRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/./a.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("path traversal"));
    }

    public void testMissingRepositoryRejected() {
        Map<String, Object> params = baseUrl();
        params.remove("repository");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("repository"));
    }

    public void testBlankRepositoryRejected() {
        Map<String, Object> params = baseUrl();
        params.put("repository", "   ");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("repository"));
    }

    public void testNeitherUrlNorPrefixRejected() {
        Map<String, Object> params = baseUrl();
        params.remove("source_url");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("neither was provided"));
    }

    public void testBothUrlAndPrefixRejected() {
        Map<String, Object> params = baseUrl();
        params.put("source_prefix", "dump/");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("both were provided"));
    }

    public void testEqualBytesWithPrefixRejected() {
        Map<String, Object> params = basePrefix();
        params.put("shard_split", "equal_bytes");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("equal_bytes"));
        assertThat(e.getMessage(), containsString("source_url"));
    }

    public void testWholeFilesWithUrlRejected() {
        Map<String, Object> params = baseUrl();
        params.put("shard_split", "whole_files");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("whole_files"));
        assertThat(e.getMessage(), containsString("source_prefix"));
    }

    public void testShardPrefixesWithUrlRejected() {
        Map<String, Object> params = baseUrl();
        params.put("shard_split", "shard_prefixes");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("shard_prefixes"));
        assertThat(e.getMessage(), containsString("source_prefix"));
    }

    public void testBadFormatRejected() {
        Map<String, Object> params = baseUrl();
        params.put("format", "parquet");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("ndjson"));
    }

    public void testBlankFormatRejected() {
        Map<String, Object> params = baseUrl();
        params.put("format", "   ");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("ndjson"));
    }

    public void testUrlWithPathTraversalRejected() {
        Map<String, Object> params = baseUrl();
        params.put("source_url", "dump/../a.ndjson");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString(".."));
    }

    public void testUrlWithLeadingSlashRejected() {
        Map<String, Object> params = baseUrl();
        params.put("source_url", "/dump/a.ndjson");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("leading"));
    }

    public void testPrefixWithWildcardRejected() {
        Map<String, Object> params = basePrefix();
        params.put("source_prefix", "dump/*");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("wildcard"));
    }

    public void testPrefixWithQuestionMarkWildcardRejected() {
        Map<String, Object> params = basePrefix();
        params.put("source_prefix", "dump/part?");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("wildcard"));
    }

    public void testZeroMaxRecordBytesRejected() {
        Map<String, Object> params = baseUrl();
        params.put("max_record_bytes", 0L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("max_record_bytes"));
    }

    public void testZeroMaxObjectsRejected() {
        Map<String, Object> params = baseUrl();
        params.put("max_objects", 0L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("[max_objects] must be greater than 0"));
    }

    public void testZeroMaxListRequestsRejected() {
        Map<String, Object> params = baseUrl();
        params.put("max_list_requests", 0L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("max_list_requests"));
    }

    public void testNegativeMaxGetRequestsRejected() {
        Map<String, Object> params = baseUrl();
        params.put("max_get_requests", -1L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("max_get_requests"));
    }

    public void testZeroMaxBytesRejected() {
        Map<String, Object> params = baseUrl();
        params.put("max_bytes", 0L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("max_bytes"));
    }

    public void testValidCostCapsPass() {
        Map<String, Object> params = baseUrl();
        params.put("max_list_requests", 10L);
        params.put("max_get_requests", 100L);
        params.put("max_bytes", 1_000_000L);
        ObjectStoreSourceConfig config = config(params);
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getMaxListRequests(), is(10L));
        assertThat(config.getMaxGetRequests(), is(100L));
        assertThat(config.getMaxBytes(), is(1_000_000L));
    }

    public void testMissingManifestRejected() {
        Map<String, Object> params = baseUrl();
        params.remove("manifest");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testBlankManifestRejected() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "   ");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testMalformedManifestRejected() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[{\"key\":");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testManifestKeyWithPathTraversalRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/../secret.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString(".."));
    }

    public void testManifestKeyWithLeadingSlashRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"/dump/part-0.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("leading"));
    }

    public void testManifestKeyWithWildcardRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/part-*.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("wildcard"));
    }

    public void testManifestKeyWithQuestionMarkWildcardRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump/part-?.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("wildcard"));
    }

    public void testEqualBytesManifestWithMultipleEntriesRejected() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[{\"key\":\"dump/events.ndjson\",\"size\":100},{\"key\":\"dump/other.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("exactly one entry"));
        assertThat(e.getMessage(), containsString("got [2] entries"));
        assertThat(e.getMessage(), containsString("the first being [dump/events.ndjson]"));
        assertThat(e.getMessage(), not(containsString("dump/other.ndjson")));
    }

    public void testEqualBytesManifestWithNoEntriesRejectedWithoutNamingAKey() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("got [0] entries"));
        assertThat(e.getMessage(), not(containsString("the first being")));
    }

    public void testEqualBytesObjectDeclaredEmptyRejected() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[{\"key\":\"dump/events.ndjson\",\"size\":0}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("declares it as 0 bytes"));
        assertThat(e.getMessage(), containsString("ingested no documents"));
    }

    public void testEqualBytesObjectWithOneByteAccepted() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[{\"key\":\"dump/events.ndjson\",\"size\":1}]");
        ObjectStoreSourceConfig config = config(params);
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getShardSplit(), is(ShardSplit.EQUAL_BYTES));
    }

    public void testEqualBytesManifestKeyNotMatchingSourceUrlRejected() {
        Map<String, Object> params = baseUrl();
        params.put("manifest", "[{\"key\":\"dump/other.ndjson\",\"size\":100}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("source_url"));
    }

    public void testWholeFilesManifestKeyOutsidePrefixRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"other/part-0.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("must start with"));
    }

    public void testManifestExceedingMaxObjectsRejected() {
        Map<String, Object> params = basePrefix();
        params.put("max_objects", 1L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("cardinality"));
    }

    public void testShardPrefixesManifestValidCasePasses() {
        Map<String, Object> params = basePrefix();
        params.put("shard_split", "shard_prefixes");
        params.put("manifest", "[{\"key\":\"dump/0/part-0.ndjson\",\"size\":10}]");
        ObjectStoreSourceConfig config = config(params);
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getShardSplit(), is(ShardSplit.SHARD_PREFIXES));
        assertThat(ManifestParser.parse(config.getManifestJson(), config.getMaxObjects()).keys(), contains("dump/0/part-0.ndjson"));
    }

    public void testPointerResetLatestRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validatePointerReset(StreamPoller.ResetState.LATEST)
        );
        assertThat(e.getMessage(), containsString("latest"));
    }

    public void testPointerResetByTimestampRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validatePointerReset(StreamPoller.ResetState.RESET_BY_TIMESTAMP)
        );
        assertThat(e.getMessage(), containsString("reset_by_timestamp"));
    }

    public void testSupportedPointerResetsPass() {
        Set<StreamPoller.ResetState> rejected = Set.of(StreamPoller.ResetState.LATEST, StreamPoller.ResetState.RESET_BY_TIMESTAMP);
        Set<StreamPoller.ResetState> accepted = new HashSet<>();
        for (StreamPoller.ResetState state : StreamPoller.ResetState.values()) {
            if (rejected.contains(state)) {
                expectThrows(IllegalArgumentException.class, () -> ObjectStoreSourceConfigValidator.validatePointerReset(state));
            } else {
                ObjectStoreSourceConfigValidator.validatePointerReset(state);
                accepted.add(state);
            }
        }
        ObjectStoreSourceConfigValidator.validatePointerReset(null);
        assertThat(
            accepted,
            containsInAnyOrder(StreamPoller.ResetState.EARLIEST, StreamPoller.ResetState.RESET_BY_OFFSET, StreamPoller.ResetState.NONE)
        );
    }

    public void testIndexLevelResetByOffsetRejectedForMultipleShards() {
        for (ShardSplit split : ShardSplit.values()) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ObjectStoreSourceConfigValidator.validatePointerResetForShards(StreamPoller.ResetState.RESET_BY_OFFSET, split, 2)
            );
            assertThat(e.getMessage(), containsString("reset_by_offset"));
            assertThat(e.getMessage(), containsString("[2] shards and [shard_split=" + split.getName() + "]"));
            assertThat(e.getMessage(), containsString("_resume"));
        }
    }

    public void testIndexLevelResetByOffsetRejectionNamesNotOwnedForWholeObjectSplits() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validatePointerResetForShards(
                StreamPoller.ResetState.RESET_BY_OFFSET,
                ShardSplit.WHOLE_FILES,
                3
            )
        );
        assertThat(e.getMessage(), containsString("rejects the pointer as not owned"));
    }

    public void testIndexLevelResetByOffsetRejectionNamesPartialSkipForEqualBytes() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validatePointerResetForShards(
                StreamPoller.ResetState.RESET_BY_OFFSET,
                ShardSplit.EQUAL_BYTES,
                3
            )
        );
        assertThat(e.getMessage(), containsString("only within the owning shard's range"));
    }

    public void testIndexLevelResetByOffsetAcceptedForSingleShard() {
        for (ShardSplit split : ShardSplit.values()) {
            ObjectStoreSourceConfigValidator.validatePointerResetForShards(StreamPoller.ResetState.RESET_BY_OFFSET, split, 1);
        }
    }

    public void testOtherResetsAcceptedForMultipleShards() {
        Set<StreamPoller.ResetState> accepted = new HashSet<>();
        for (StreamPoller.ResetState state : StreamPoller.ResetState.values()) {
            if (state != StreamPoller.ResetState.RESET_BY_OFFSET) {
                ObjectStoreSourceConfigValidator.validatePointerResetForShards(state, ShardSplit.WHOLE_FILES, 4);
                accepted.add(state);
            }
        }
        ObjectStoreSourceConfigValidator.validatePointerResetForShards(null, ShardSplit.WHOLE_FILES, 4);
        assertThat(
            accepted,
            containsInAnyOrder(
                StreamPoller.ResetState.EARLIEST,
                StreamPoller.ResetState.LATEST,
                StreamPoller.ResetState.RESET_BY_TIMESTAMP,
                StreamPoller.ResetState.NONE
            )
        );
    }

    public void testUrlWithInternalDoubleSlashRejected() {
        Map<String, Object> params = baseUrl();
        params.put("source_url", "a//b.ndjson");
        params.put("manifest", "[{\"key\":\"a//b.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("empty path segments"));
    }

    public void testManifestKeyWithInternalDoubleSlashRejected() {
        Map<String, Object> params = basePrefix();
        params.put("manifest", "[{\"key\":\"dump//part-0.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("empty path segments"));
    }

    public void testUnicodeNormalizedTraversalRejected() {
        Map<String, Object> params = baseUrl();
        params.put("source_url", "dump/．．/events.ndjson");
        params.put("manifest", "[{\"key\":\"dump/．．/events.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("path traversal"));
    }

    public void testFieldMappingMapperRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validateMapper(IngestionMessageMapper.MapperType.FIELD_MAPPING)
        );
        assertThat(e.getMessage(), containsString("field_mapping"));
        assertThat(e.getMessage(), containsString("index.ingestion_source.mapper_type: raw_payload"));
    }

    public void testOnlyRawPayloadMapperPasses() {
        Set<IngestionMessageMapper.MapperType> accepted = new HashSet<>();
        for (IngestionMessageMapper.MapperType type : IngestionMessageMapper.MapperType.values()) {
            if (type == IngestionMessageMapper.MapperType.RAW_PAYLOAD) {
                ObjectStoreSourceConfigValidator.validateMapper(type);
                accepted.add(type);
            } else {
                expectThrows(IllegalArgumentException.class, () -> ObjectStoreSourceConfigValidator.validateMapper(type));
            }
        }
        assertThat(accepted, contains(IngestionMessageMapper.MapperType.RAW_PAYLOAD));
    }

    public void testDefaultMapperRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validateMapper(IngestionMessageMapper.MapperType.DEFAULT)
        );
        assertThat(e.getMessage(), containsString("mapper_type [default]"));
        assertThat(e.getMessage(), containsString("unset mapper_type"));
        assertThat(e.getMessage(), containsString("index.ingestion_source.mapper_type: raw_payload"));
    }

    public void testUnsetMapperRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validateMapper(null)
        );
        assertThat(e.getMessage(), containsString("mapper_type [unset]"));
        assertThat(e.getMessage(), containsString("index.ingestion_source.mapper_type: raw_payload"));
    }

    public void testZeroMaxObjectsRejectedWithPreParsedManifest() {
        Map<String, Object> params = baseUrl();
        params.put("max_objects", 0L);
        ObjectStoreManifest manifest = new ObjectStoreManifest(Map.of("dump/events.ndjson", 100L));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params), manifest)
        );
        assertThat(e.getMessage(), containsString("[max_objects] must be greater than 0"));
    }

    private static Map<String, Object> baseList() {
        Map<String, Object> params = new HashMap<>();
        params.put("repository", "repo");
        params.put("format", "ndjson");
        params.put("shard_split", "shard_prefixes");
        params.put("source_prefix", "dump/");
        params.put("discovery", "list");
        return params;
    }

    public void testListDiscoveryWithShardPrefixesAndNoManifestPasses() {
        ObjectStoreSourceConfig config = config(baseList());
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getDiscovery(), is(Discovery.LIST));
        assertThat(config.getShardSplit(), is(ShardSplit.SHARD_PREFIXES));
        assertThat(config.getManifestJson(), is(nullValue()));
    }

    public void testListDiscoveryRejectedWithEqualBytes() {
        Map<String, Object> params = baseList();
        params.remove("source_prefix");
        params.put("source_url", "dump/events.ndjson");
        params.put("shard_split", "equal_bytes");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("[discovery=list] requires [shard_split=shard_prefixes]"));
        assertThat(e.getMessage(), containsString("got [equal_bytes]"));
    }

    public void testListDiscoveryRejectedWithWholeFiles() {
        Map<String, Object> params = baseList();
        params.put("shard_split", "whole_files");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("got [whole_files]"));
    }

    public void testListDiscoveryRejectsAnExplicitManifest() {
        Map<String, Object> params = baseList();
        params.put("manifest", "[{\"key\":\"dump/0/a.ndjson\",\"size\":10}]");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("[manifest] must not be set when [discovery=list]"));
    }

    public void testListDiscoveryStillEnforcesPathSafety() {
        Map<String, Object> params = baseList();
        params.put("source_prefix", "../dump/");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("path traversal"));
    }

    public void testListDiscoveryStillEnforcesCostCaps() {
        Map<String, Object> params = baseList();
        params.put("max_list_requests", 0L);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("max_list_requests"));
    }

    public void testManifestDiscoveryStillRequiresAManifest() {
        Map<String, Object> params = baseList();
        params.put("discovery", "manifest");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("[manifest] is required"));
    }

    public void testManifestOverTheByteCapRejected() {
        StringBuilder manifest = new StringBuilder("[");
        int entry = 0;
        while (manifest.length() <= ObjectStoreSourceConfig.MAX_MANIFEST_BYTES) {
            if (entry > 0) {
                manifest.append(',');
            }
            manifest.append("{\"key\":\"dump/part-").append(entry++).append(".ndjson\",\"size\":10}");
        }
        manifest.append(']');
        Map<String, Object> params = basePrefix();
        params.put("manifest", manifest.toString());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ObjectStoreSourceConfigValidator.validate(config(params))
        );
        assertThat(e.getMessage(), containsString("byte cap"));
        assertThat(e.getMessage(), containsString("discovery=list"));
    }

    public void testManifestUnderTheByteCapAccepted() {
        ObjectStoreSourceConfig config = config(basePrefix());
        ObjectStoreSourceConfigValidator.validate(config);
        assertThat(config.getManifestJson().length(), lessThan(ObjectStoreSourceConfig.MAX_MANIFEST_BYTES));
    }

}
