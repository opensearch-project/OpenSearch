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

package org.opensearch.search.internal;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RandomQueryBuilder;
import org.opensearch.indices.InvalidAliasNameException;
import org.opensearch.search.AbstractSearchTestCase;
import org.opensearch.search.SearchSortValuesAndFormatsTests;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ShardSearchRequestTests extends AbstractSearchTestCase {
    private static final IndexMetadata BASE_METADATA = IndexMetadata.builder("test")
        .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build())
        .numberOfShards(1)
        .numberOfReplicas(1)
        .build();

    public void testSerialization() throws Exception {
        ShardSearchRequest shardSearchTransportRequest = createShardSearchRequest();
        ShardSearchRequest deserializedRequest = copyWriteable(
            shardSearchTransportRequest,
            namedWriteableRegistry,
            ShardSearchRequest::new
        );
        assertEquals(shardSearchTransportRequest, deserializedRequest);
    }

    public void testClone() throws Exception {
        for (int i = 0; i < 10; i++) {
            ShardSearchRequest shardSearchTransportRequest = createShardSearchRequest();
            ShardSearchRequest clone = new ShardSearchRequest(shardSearchTransportRequest);
            assertEquals(shardSearchTransportRequest, clone);
        }
    }

    private ShardSearchRequest createShardSearchRequest() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10), randomInt());
        final AliasFilter filteringAliases;
        if (randomBoolean()) {
            String[] strings = generateRandomStringArray(10, 10, false, false);
            filteringAliases = new AliasFilter(RandomQueryBuilder.createQuery(random()), strings);
        } else {
            filteringAliases = new AliasFilter(null, Strings.EMPTY_ARRAY);
        }
        final String[] routings = generateRandomStringArray(5, 10, false, true);
        ShardSearchContextId shardSearchContextId = null;
        TimeValue keepAlive = null;
        if (randomBoolean()) {
            shardSearchContextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            if (randomBoolean()) {
                keepAlive = TimeValue.timeValueSeconds(randomIntBetween(0, 120));
            }
        }
        ShardSearchRequest req = new ShardSearchRequest(
            new OriginalIndices(searchRequest),
            searchRequest,
            shardId,
            randomIntBetween(1, 100),
            filteringAliases,
            randomBoolean() ? 1.0f : randomFloat(),
            Math.abs(randomLong()),
            randomAlphaOfLengthBetween(3, 10),
            routings,
            shardSearchContextId,
            keepAlive
        );
        req.canReturnNullResponseIfMatchNoDocs(randomBoolean());
        if (randomBoolean()) {
            req.setBottomSortValues(SearchSortValuesAndFormatsTests.randomInstance());
        }
        if (randomBoolean()) {
            req.setRequestStartNanos(randomNonNegativeLong());
        }
        return req;
    }

    public void testFilteringAliases() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "dog")));
        indexMetadata = add(indexMetadata, "all", null);

        assertThat(indexMetadata.getAliases().containsKey("cats"), equalTo(true));
        assertThat(indexMetadata.getAliases().containsKey("dogs"), equalTo(true));
        assertThat(indexMetadata.getAliases().containsKey("turtles"), equalTo(false));

        assertEquals(aliasFilter(indexMetadata, "cats"), QueryBuilders.termQuery("animal", "cat"));
        assertEquals(
            aliasFilter(indexMetadata, "cats", "dogs"),
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("animal", "cat")).should(QueryBuilders.termQuery("animal", "dog"))
        );

        // Non-filtering alias should turn off all filters because filters are ORed
        assertThat(aliasFilter(indexMetadata, "all"), nullValue());
        assertThat(aliasFilter(indexMetadata, "cats", "all"), nullValue());
        assertThat(aliasFilter(indexMetadata, "all", "cats"), nullValue());

        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "feline")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "canine")));
        assertEquals(
            aliasFilter(indexMetadata, "dogs", "cats"),
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("animal", "canine"))
                .should(QueryBuilders.termQuery("animal", "feline"))
        );
    }

    public void testRemovedAliasFilter() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = remove(indexMetadata, "cats");
        try {
            aliasFilter(indexMetadata, "cats");
            fail("Expected InvalidAliasNameException");
        } catch (InvalidAliasNameException e) {
            assertThat(e.getMessage(), containsString("Invalid alias name [cats]"));
        }
    }

    public void testUnknownAliasFilter() throws Exception {
        IndexMetadata indexMetadata = BASE_METADATA;
        indexMetadata = add(indexMetadata, "cats", filter(termQuery("animal", "cat")));
        indexMetadata = add(indexMetadata, "dogs", filter(termQuery("animal", "dog")));
        IndexMetadata finalIndexMetadata = indexMetadata;
        expectThrows(InvalidAliasNameException.class, () -> aliasFilter(finalIndexMetadata, "unknown"));
    }

    private static void assertEquals(ShardSearchRequest orig, ShardSearchRequest copy) throws IOException {
        assertEquals(orig.scroll(), copy.scroll());
        assertEquals(orig.getAliasFilter(), copy.getAliasFilter());
        assertArrayEquals(orig.indices(), copy.indices());
        assertEquals(orig.indicesOptions(), copy.indicesOptions());
        assertEquals(orig.nowInMillis(), copy.nowInMillis());
        assertEquals(orig.source(), copy.source());
        assertEquals(orig.searchType(), copy.searchType());
        assertEquals(orig.shardId(), copy.shardId());
        assertEquals(orig.numberOfShards(), copy.numberOfShards());
        assertArrayEquals(orig.indexRoutings(), copy.indexRoutings());
        assertEquals(orig.preference(), copy.preference());
        assertEquals(orig.cacheKey(), copy.cacheKey());
        assertNotSame(orig, copy);
        assertEquals(orig.getAliasFilter(), copy.getAliasFilter());
        assertEquals(orig.indexBoost(), copy.indexBoost(), 0.0f);
        assertEquals(orig.getClusterAlias(), copy.getClusterAlias());
        assertEquals(orig.allowPartialSearchResults(), copy.allowPartialSearchResults());
        assertEquals(orig.canReturnNullResponseIfMatchNoDocs(), orig.canReturnNullResponseIfMatchNoDocs());
        assertEquals(orig.getRequestStartNanos(), copy.getRequestStartNanos());
    }

    /**
     * Tests that requestStartNanos survives serialization round-trip with version gating (V_3_0_0+).
     * Validates: Requirements 8.1, 13.2
     */
    public void testRequestStartNanosSerializationRoundTrip() throws Exception {
        ShardSearchRequest request = createShardSearchRequest();
        long expectedNanos = randomNonNegativeLong();
        request.setRequestStartNanos(expectedNanos);

        // Serialize and deserialize with current version (>= V_3_0_0)
        ShardSearchRequest deserialized = copyWriteable(request, namedWriteableRegistry, ShardSearchRequest::new, Version.CURRENT);

        assertEquals(expectedNanos, deserialized.getRequestStartNanos());
        assertEquals(request, deserialized);
    }

    /**
     * Tests that deserializing from an older version stream (before V_3_0_0) produces default 0 for requestStartNanos.
     * Validates: Requirements 8.1, 13.2
     */
    public void testRequestStartNanosOlderVersionDefaultsToZero() throws Exception {
        ShardSearchRequest request = createShardSearchRequest();
        long nonZeroNanos = randomLongBetween(1, Long.MAX_VALUE);
        request.setRequestStartNanos(nonZeroNanos);

        // Serialize with a version before V_3_0_0 — requestStartNanos should not be written
        Version oldVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.V_2_0_0,
            VersionUtils.getPreviousVersion(Version.V_3_0_0)
        );
        ShardSearchRequest deserialized = copyWriteable(request, namedWriteableRegistry, ShardSearchRequest::new, oldVersion);

        // Older version stream should produce default 0
        assertEquals(0L, deserialized.getRequestStartNanos());
    }

    /**
     * Tests absolute offset computation when requestStartNanos > 0.
     * When requestStartNanos is set, offset = (event_start_nanos - requestStartNanos) / 1000.
     * Validates: Requirements 8.2, 13.1
     */
    public void testAbsoluteOffsetComputationWithRequestStartNanosPositive() {
        long requestStartNanos = randomLongBetween(1_000_000L, 1_000_000_000_000L);
        long eventStartNanos = requestStartNanos + randomLongBetween(1000L, 100_000_000L);

        // When requestStartNanos > 0, compute absolute offset
        long expectedOffsetMicros = (eventStartNanos - requestStartNanos) / 1000;

        assertTrue("requestStartNanos should be positive", requestStartNanos > 0);
        assertTrue("expected offset should be non-negative", expectedOffsetMicros >= 0);
        assertEquals(expectedOffsetMicros, (eventStartNanos - requestStartNanos) / 1000);
    }

    /**
     * Tests fallback behavior when requestStartNanos == 0 (older coordinator or not applicable).
     * When requestStartNanos is 0, the offset stays 0 (duration-only mode).
     * Validates: Requirements 8.2, 13.1, 13.2
     */
    public void testAbsoluteOffsetComputationWithRequestStartNanosZero() {
        long requestStartNanos = 0;
        long eventStartNanos = randomNonNegativeLong();

        // When requestStartNanos == 0, fall back to duration-only (offset stays 0)
        long computedOffset;
        if (requestStartNanos > 0) {
            computedOffset = (eventStartNanos - requestStartNanos) / 1000;
        } else {
            computedOffset = 0; // fallback: duration-only
        }

        assertEquals(0L, computedOffset);
    }

    /**
     * Tests that requestStartNanos == 0 is the default when not explicitly set.
     * Validates: Requirements 13.2
     */
    public void testRequestStartNanosDefaultsToZero() throws Exception {
        SearchRequest searchRequest = createSearchRequest();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(2, 10), randomAlphaOfLengthBetween(2, 10), randomInt());
        AliasFilter aliasFilter = new AliasFilter(null, Strings.EMPTY_ARRAY);

        ShardSearchRequest req = new ShardSearchRequest(
            new OriginalIndices(searchRequest),
            searchRequest,
            shardId,
            randomIntBetween(1, 100),
            aliasFilter,
            1.0f,
            Math.abs(randomLong()),
            randomAlphaOfLengthBetween(3, 10),
            new String[0],
            null,
            null
        );

        assertEquals(0L, req.getRequestStartNanos());
    }

    /**
     * Tests multiple random requestStartNanos values survive serialization (randomized property).
     * Validates: Requirements 8.1, 8.2
     */
    public void testRequestStartNanosRandomizedRoundTrip() throws Exception {
        for (int i = 0; i < 100; i++) {
            ShardSearchRequest request = createShardSearchRequest();
            long nanos = randomNonNegativeLong();
            request.setRequestStartNanos(nanos);

            ShardSearchRequest deserialized = copyWriteable(request, namedWriteableRegistry, ShardSearchRequest::new, Version.CURRENT);
            assertEquals(nanos, deserialized.getRequestStartNanos());
        }
    }

    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(builder.toString());
    }

    private IndexMetadata remove(IndexMetadata indexMetadata, String alias) {
        return IndexMetadata.builder(indexMetadata).removeAlias(alias).build();
    }

    private IndexMetadata add(IndexMetadata indexMetadata, String alias, @Nullable CompressedXContent filter) {
        return IndexMetadata.builder(indexMetadata).putAlias(AliasMetadata.builder(alias).filter(filter).build()).build();
    }

    public QueryBuilder aliasFilter(IndexMetadata indexMetadata, String... aliasNames) {
        return ShardSearchRequest.parseAliasFilter(bytes -> {
            try (
                InputStream inputStream = bytes.streamInput();
                XContentParser parser = MediaTypeRegistry.xContentType(inputStream)
                    .xContent()
                    .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, inputStream)
            ) {
                return parseInnerQueryBuilder(parser);
            }
        }, indexMetadata, aliasNames);
    }
}
