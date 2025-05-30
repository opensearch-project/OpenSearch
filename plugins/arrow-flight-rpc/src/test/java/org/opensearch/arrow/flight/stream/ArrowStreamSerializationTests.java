/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ArrowStreamSerializationTests extends OpenSearchTestCase {
    private NamedWriteableRegistry registry;
    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(StringTerms.class, StringTerms.NAME, StringTerms::new),
                new NamedWriteableRegistry.Entry(InternalAggregation.class, StringTerms.NAME, StringTerms::new),
                new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), (si) -> DocValueFormat.RAW)
            )
        );
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        allocator.close();
    }

    public void testInternalAggregationSerializationDeserialization() throws IOException {
        StringTerms original = createTestStringTerms();
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            output.writeNamedWriteable(original);
            VectorSchemaRoot unifiedRoot = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(unifiedRoot, registry)) {
                StringTerms deserialized = input.readNamedWriteable(StringTerms.class);
                assertEquals(String.valueOf(original), String.valueOf(deserialized));
            }
        }
    }

    public void testQuerySearchResult() throws IOException {
        QuerySearchResult original = createQuerySearchResult();
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            original.writeTo(output);
            VectorSchemaRoot unifiedRoot = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(unifiedRoot, registry)) {
                QuerySearchResult deserialized = new QuerySearchResult(input);

                assertNotNull(deserialized);
                assertEquals(original.isNull(), deserialized.isNull());

                assertEquals(original.getContextId(), deserialized.getContextId());
                assertEquals(original.from(), deserialized.from());
                assertEquals(original.size(), deserialized.size());

                // Compare TopDocsAndMaxScore
                assertTopDocsAndMaxScoreEquals(original.topDocs(), deserialized.topDocs());

                if (original.sortValueFormats() != null) {
                    assertNotNull(deserialized.sortValueFormats());
                    assertArrayEquals(original.sortValueFormats(), deserialized.sortValueFormats());
                } else {
                    assertNull(deserialized.sortValueFormats());
                }

                if (original.hasAggs()) {
                    assertTrue(deserialized.hasAggs());
                    assertNotNull(deserialized.aggregations());
                    assertEquals(
                        String.valueOf(original.aggregations().getAsMap()),
                        String.valueOf(deserialized.aggregations().getAsMap())
                    );
                } else {
                    assertFalse(deserialized.hasAggs());
                    assertNull(deserialized.aggregations());
                }

                if (original.suggest() != null) {
                    assertNotNull(deserialized.suggest());
                } else {
                    assertNull(deserialized.suggest());
                }

                assertEquals(original.searchTimedOut(), deserialized.searchTimedOut());
                assertEquals(original.terminatedEarly(), deserialized.terminatedEarly());

                if (original.hasProfileResults()) {
                    assertTrue(deserialized.hasProfileResults());
                    assertNotNull(deserialized);
                } else {
                    assertFalse(deserialized.hasProfileResults());
                }

                assertEquals(original.serviceTimeEWMA(), deserialized.serviceTimeEWMA());
                assertEquals(original.nodeQueueSize(), deserialized.nodeQueueSize());
            }
        }
    }

    private void assertTopDocsAndMaxScoreEquals(TopDocsAndMaxScore expected, TopDocsAndMaxScore actual) {
        if (expected == null) {
            assertNull(actual);
            return;
        }
        assertNotNull(actual);

        if (Float.isNaN(expected.maxScore)) {
            assertTrue(Float.isNaN(actual.maxScore));
        } else {
            assertEquals(expected.maxScore, actual.maxScore, 0.0f);
        }

        assertTopDocsEquals(expected.topDocs, actual.topDocs);
    }

    private void assertTopDocsEquals(TopDocs expected, TopDocs actual) {
        if (expected == null) {
            assertNull(actual);
            return;
        }
        assertNotNull(actual);

        assertEquals(expected.totalHits.value(), actual.totalHits.value());
        assertEquals(expected.totalHits.relation(), actual.totalHits.relation());

        assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
            assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0.0f);
            assertEquals(expected.scoreDocs[i].shardIndex, actual.scoreDocs[i].shardIndex);
        }
    }

    private QuerySearchResult createQuerySearchResult() {
        QuerySearchResult result = new QuerySearchResult(createShardSearchContextId(), searchShardTarget(), shardSearchRequest());
        result.size(0);
        InternalAggregations aggregations = new InternalAggregations(List.of(createTestStringTerms()));
        result.aggregations(aggregations);

        TotalHits totalHits = new TotalHits(2, TotalHits.Relation.EQUAL_TO);
        ScoreDoc[] scoreDocs = new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, 2.0f) };
        TopDocs topDocs = new TopDocs(totalHits, scoreDocs);
        TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, Float.NaN);
        result.topDocs(topDocsAndMaxScore, new DocValueFormat[] { DocValueFormat.RAW });
        return result;
    }

    private ShardSearchContextId createShardSearchContextId() {
        return new ShardSearchContextId(randomAlphaOfLength(10), randomInt());
    }

    private SearchShardTarget searchShardTarget() {
        return new SearchShardTarget(
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(1000)),
            randomAlphaOfLength(10),
            new OriginalIndices(new String[] { "test" }, IndicesOptions.LENIENT_EXPAND_OPEN)
        );
    }

    private ShardSearchRequest shardSearchRequest() {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "test" }, IndicesOptions.LENIENT_EXPAND_OPEN);
        ShardId shardId = new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(1000));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        return new ShardSearchRequest(
            originalIndices,
            searchRequest,
            shardId,
            randomInt(100),
            AliasFilter.EMPTY,
            1.0f,
            Long.MAX_VALUE,
            randomAlphaOfLength(100),
            new String[] { "test" }
        );
    }

    private StringTerms createTestStringTerms() {
        return new StringTerms(
            "agg1",
            InternalOrder.key(true),
            InternalOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            10,
            false,
            50,
            Arrays.asList(
                new StringTerms.Bucket(
                    new BytesRef("term1"),
                    100,
                    InternalAggregations.from(
                        Collections.singletonList(
                            new StringTerms(
                                "sub_agg_1",
                                InternalOrder.key(true),
                                InternalOrder.key(true),
                                Collections.emptyMap(),
                                DocValueFormat.RAW,
                                10,
                                false,
                                10,
                                Arrays.asList(
                                    new StringTerms.Bucket(
                                        new BytesRef("subterm1_1"),
                                        30,
                                        InternalAggregations.EMPTY,
                                        false,
                                        0,
                                        DocValueFormat.RAW
                                    )
                                ),
                                0,
                                new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
                            )
                        )
                    ),
                    false,
                    0,
                    DocValueFormat.RAW
                ),
                new StringTerms.Bucket(
                    new BytesRef("term2"),
                    100,
                    InternalAggregations.from(
                        Collections.singletonList(
                            new StringTerms(
                                "sub_agg_2",
                                InternalOrder.key(true),
                                InternalOrder.key(true),
                                Collections.emptyMap(),
                                DocValueFormat.RAW,
                                10,
                                false,
                                19,
                                Arrays.asList(
                                    new StringTerms.Bucket(
                                        new BytesRef("subterm2_1"),
                                        31,
                                        InternalAggregations.EMPTY,
                                        false,
                                        101,
                                        DocValueFormat.RAW
                                    )
                                ),
                                0,
                                new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
                            )
                        )
                    ),
                    false,
                    0,
                    DocValueFormat.RAW
                )
            ),
            0,
            new TermsAggregator.BucketCountThresholds(10, 0, 10, 10)
        );
    }
}
