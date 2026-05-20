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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.opensearch.search.aggregations.metrics.Avg;
import org.opensearch.search.aggregations.metrics.ExtendedStats;
import org.opensearch.search.aggregations.metrics.Stats;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.avg;
import static org.opensearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.opensearch.search.aggregations.AggregationBuilders.filter;
import static org.opensearch.search.aggregations.AggregationBuilders.stats;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsNull.notNullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class StringTermsIT extends BaseStringTermsTestCase {

    public StringTermsIT(Settings staticSettings) {
        super(staticSettings);
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        final int minDocCount = randomInt(1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("high_card_idx")
                .addAggregation(
                    terms("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .minDocCount(minDocCount)
                        .size(0)
                )
                .get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [terms]"));
    }

    public void testSingleValueFieldWithPartitionedFiltering() throws Exception {
        runTestFieldWithPartitionedFiltering(SINGLE_VALUED_FIELD_NAME);
    }

    public void testMultiValueFieldWithPartitionedFiltering() throws Exception {
        runTestFieldWithPartitionedFiltering(MULTI_VALUED_FIELD_NAME);
    }

    private void runTestFieldWithPartitionedFiltering(String field) throws Exception {
        // Find total number of unique terms
        SearchResponse allResponse = client().prepareSearch("idx")
            .addAggregation(terms("terms").field(field).size(10000).collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();
        assertSearchResponse(allResponse);
        Terms terms = allResponse.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        int expectedCardinality = terms.getBuckets().size();

        // Gather terms using partitioned aggregations
        final int numPartitions = randomIntBetween(2, 4);
        Set<String> foundTerms = new HashSet<>();
        for (int partition = 0; partition < numPartitions; partition++) {
            SearchResponse response = client().prepareSearch("idx")
                .addAggregation(
                    terms("terms").field(field)
                        .includeExclude(new IncludeExclude(partition, numPartitions))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                )
                .get();
            assertSearchResponse(response);
            terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            for (Bucket bucket : terms.getBuckets()) {
                assertTrue(foundTerms.add(bucket.getKeyAsString()));
            }
        }
        assertEquals(expectedCardinality, foundTerms.size());
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testMultiValuedFieldWithValueScriptNotUnique() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value.substring(0,3)", Collections.emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(1));

        Terms.Bucket bucket = terms.getBucketByKey("val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("val"));
        assertThat(bucket.getDocCount(), equalTo(5L));
    }

    public void testMultiValuedScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .script(
                        new Script(
                            ScriptType.INLINE,
                            CustomScriptPlugin.NAME,
                            "doc['" + MULTI_VALUED_FIELD_NAME + "']",
                            Collections.emptyMap()
                        )
                    )
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(MULTI_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("foo_val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    /*
     *
     * [foo_val0, foo_val1] [foo_val1, foo_val2] [foo_val2, foo_val3] [foo_val3,
     * foo_val4] [foo_val4, foo_val5]
     *
     *
     * foo_val0 - doc_count: 1 - val_count: 2 foo_val1 - doc_count: 2 -
     * val_count: 4 foo_val2 - doc_count: 2 - val_count: 4 foo_val3 - doc_count:
     * 2 - val_count: 4 foo_val4 - doc_count: 2 - val_count: 4 foo_val5 -
     * doc_count: 1 - val_count: 2
     */

    public void testScriptSingleValue() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint()).script(script)
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testScriptSingleValueExplicitSingleValue() throws Exception {
        Script script = new Script(
            ScriptType.INLINE,
            CustomScriptPlugin.NAME,
            "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
            Collections.emptyMap()
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").collectMode(randomFrom(SubAggCollectionMode.values())).executionHint(randomExecutionHint()).script(script)
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testScriptMultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(randomExecutionHint())
                    .script(
                        new Script(
                            ScriptType.INLINE,
                            CustomScriptPlugin.NAME,
                            "doc['" + MULTI_VALUED_FIELD_NAME + "']",
                            Collections.emptyMap()
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1L));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2L));
            }
        }
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testStringTermsNestedIntoPerBucketAggregator() throws Exception {
        // no execution hint so that the logic that decides whether or not to use ordinals is executed
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                filter("filter", termQuery(MULTI_VALUED_FIELD_NAME, "val3")).subAggregation(
                    terms("terms").field(MULTI_VALUED_FIELD_NAME).collectMode(randomFrom(SubAggCollectionMode.values()))
                )
            )
            .get();

        assertThat(response.getFailedShards(), equalTo(0));

        Filter filter = response.getAggregations().get("filter");

        Terms terms = filter.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        for (int i = 2; i <= 4; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(i == 3 ? 2L : 1L));
        }
    }

    public void testSingleValuedFieldOrderedByIllegalAgg() throws Exception {
        boolean asc = true;
        try {
            client().prepareSearch("idx")
                .addAggregation(
                    terms("terms").executionHint(randomExecutionHint())
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(BucketOrder.aggregation("inner_terms>avg", asc))
                        .subAggregation(terms("inner_terms").field(MULTI_VALUED_FIELD_NAME).subAggregation(avg("avg").field("i")))
                )
                .get();
            fail("Expected an exception");
        } catch (SearchPhaseExecutionException e) {
            OpenSearchException[] rootCauses = e.guessRootCauses();
            if (rootCauses.length == 1) {
                OpenSearchException rootCause = rootCauses[0];
                if (rootCause instanceof AggregationExecutionException) {
                    AggregationExecutionException aggException = (AggregationExecutionException) rootCause;
                    assertThat(aggException.getMessage(), startsWith("Invalid aggregation order path"));
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    public void testSingleValuedFieldOrderedBySingleBucketSubAggregationAsc() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter", asc))
                    .subAggregation(filter("filter", QueryBuilders.matchAllQuery()))
            )
            .get();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<? extends Terms.Bucket> iters = tags.getBuckets().iterator();

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        Filter filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 2L : 3L));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 3L : 2L));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevels() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>filter2>stats.max", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter("filter2", QueryBuilders.matchAllQuery()).subAggregation(stats("stats").field("i"))
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<? extends Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get("stats");
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialChars() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>" + filter2Name + ">" + statsName + ".max", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(stats(statsName).field("i"))
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<? extends Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedBySubAggregationAscMultiHierarchyLevelsSpecialCharsNoDotNotation() throws Exception {
        StringBuilder filter2NameBuilder = new StringBuilder("filt.er2");
        filter2NameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String filter2Name = filter2NameBuilder.toString();
        StringBuilder statsNameBuilder = new StringBuilder("st.ats");
        statsNameBuilder.append(randomAlphaOfLengthBetween(3, 10).replace("[", "").replace("]", "").replace(">", ""));
        String statsName = statsNameBuilder.toString();
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("tags").executionHint(randomExecutionHint())
                    .field("tag")
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("filter1>" + filter2Name + ">" + statsName + "[max]", asc))
                    .subAggregation(
                        filter("filter1", QueryBuilders.matchAllQuery()).subAggregation(
                            filter(filter2Name, QueryBuilders.matchAllQuery()).subAggregation(stats(statsName).field("i"))
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<? extends Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "more" is 2
        // the max for "less" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "more" : "less"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3L : 2L));
        Filter filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3L : 2L));
        Stats stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "less" : "more"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2L : 3L));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2L : 3L));
        filter2 = filter1.getAggregations().get(filter2Name);
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2L : 3L));
        stats = filter2.getAggregations().get(statsName);
        assertThat(stats, notNullValue());
        assertThat(stats.getMax(), equalTo(asc ? 4.0 : 2.0));
    }

    public void testSingleValuedFieldOrderedByMissingSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                    .addAggregation(
                        terms("terms").executionHint(randomExecutionHint())
                            .field(SINGLE_VALUED_FIELD_NAME)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .order(BucketOrder.aggregation("avg_i", true))
                    )
                    .get();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation that doesn't exist");

            } catch (OpenSearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByNonMetricsOrMultiBucketSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                    .addAggregation(
                        terms("terms").executionHint(randomExecutionHint())
                            .field(SINGLE_VALUED_FIELD_NAME)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .order(BucketOrder.aggregation("values", true))
                            .subAggregation(terms("values").field("i").collectMode(randomFrom(SubAggCollectionMode.values())))
                    )
                    .get();

                fail(
                    "Expected search to fail when trying to sort terms aggregation by sug-aggregation "
                        + "which is not of a metrics or single-bucket type"
                );

            } catch (OpenSearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithUnknownMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                SearchResponse response = client().prepareSearch(index)
                    .addAggregation(
                        terms("terms").executionHint(randomExecutionHint())
                            .field(SINGLE_VALUED_FIELD_NAME)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .order(BucketOrder.aggregation("stats.foo", true))
                            .subAggregation(stats("stats").field("i"))
                    )
                    .get();
                fail(
                    "Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "with an unknown specified metric to order by. response had "
                        + response.getFailedShards()
                        + " failed shards."
                );

            } catch (OpenSearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValuedSubAggregationWithoutMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index)
                    .addAggregation(
                        terms("terms").executionHint(randomExecutionHint())
                            .field(SINGLE_VALUED_FIELD_NAME)
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .order(BucketOrder.aggregation("stats", true))
                            .subAggregation(stats("stats").field("i"))
                    )
                    .execute()
                    .actionGet();

                fail(
                    "Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation "
                        + "where the metric name is not specified"
                );

            } catch (OpenSearchException e) {
                // expected
            }
        }
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field("i"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i++;
        }
    }

    public void testSingleValuedFieldOrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.avg", asc))
                    .subAggregation(stats("stats").field("i"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i--;
        }

    }

    public void testSingleValuedFieldOrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.sum_of_squares", asc))
                    .subAggregation(extendedStats("stats").field("i"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
            i++;
        }

    }

    public void testSingleValuedFieldOrderedByStatsAggAscWithTermsSubAgg() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.aggregation("stats.sum_of_squares", asc))
                    .subAggregation(extendedStats("stats").field("i"))
                    .subAggregation(terms("subTerms").field("s_values").collectMode(randomFrom(SubAggCollectionMode.values())))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));

            Terms subTermsAgg = bucket.getAggregations().get("subTerms");
            assertThat(subTermsAgg, notNullValue());
            assertThat(subTermsAgg.getBuckets().size(), equalTo(2));
            int j = i;
            for (Terms.Bucket subBucket : subTermsAgg.getBuckets()) {
                assertThat(subBucket, notNullValue());
                assertThat(key(subBucket), equalTo("val" + j));
                assertThat(subBucket.getDocCount(), equalTo(1L));
                j++;
            }
            i++;
        }

    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsDesc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val4", "val3", "val7", "val6", "val5" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(false));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationDescAndTermsAsc() throws Exception {
        String[] expectedKeys = new String[] { "val5", "val6", "val7", "val3", "val4", "val2", "val1" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", false), BucketOrder.key(true));
    }

    public void testSingleValuedFieldOrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val4", "val5", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, BucketOrder.count(true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        String[] expectedKeys = new String[] { "val6", "val7", "val3", "val5", "val4", "val1", "val2" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("sum_d", true), BucketOrder.aggregation("avg_l", true));
    }

    public void testSingleValuedFieldOrderedByThreeCriteria() throws Exception {
        String[] expectedKeys = new String[] { "val2", "val1", "val4", "val5", "val3", "val6", "val7" };
        assertMultiSortResponse(
            expectedKeys,
            BucketOrder.count(false),
            BucketOrder.aggregation("sum_d", false),
            BucketOrder.aggregation("avg_l", false)
        );
    }

    public void testSingleValuedFieldOrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        String[] expectedKeys = new String[] { "val1", "val2", "val3", "val4", "val5", "val6", "val7" };
        assertMultiSortResponse(expectedKeys, BucketOrder.aggregation("avg_l", true));
    }

    private void assertMultiSortResponse(String[] expectedKeys, BucketOrder... order) {
        SearchResponse response = client().prepareSearch("sort_idx")
            .addAggregation(
                terms("terms").executionHint(randomExecutionHint())
                    .field(SINGLE_VALUED_FIELD_NAME)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.compound(order))
                    .subAggregation(avg("avg_l").field("l"))
                    .subAggregation(sum("sum_d").field("d"))
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(expectedKeys.length));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(expectedKeys[i]));
            assertThat(bucket.getDocCount(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("_count")));
            Avg avg = bucket.getAggregations().get("avg_l");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("avg_l")));
            Sum sum = bucket.getAggregations().get("sum_d");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("sum_d")));
            i++;
        }
    }

    public void testIndexMetaField() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "empty_bucket_idx")
            .addAggregation(
                terms("terms").collectMode(randomFrom(SubAggCollectionMode.values()))
                    .executionHint(randomExecutionHint())
                    .field(IndexFieldMapper.NAME)
            )
            .get();

        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(2));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i == 0 ? "idx" : "empty_bucket_idx"));
            assertThat(bucket.getDocCount(), equalTo(i == 0 ? 5L : 2L));
            i++;
        }
    }

    public void testOtherDocCount() {
        testOtherDocCount(SINGLE_VALUED_FIELD_NAME, MULTI_VALUED_FIELD_NAME);
    }

    public void testDeferredSubAggs() {
        // Tests subAgg doc count is the same with different collection modes and additional top level aggs
        SearchResponse r1 = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(
                terms("terms1").collectMode(SubAggCollectionMode.BREADTH_FIRST)
                    .field("s_value")
                    .size(2)
                    .subAggregation(AggregationBuilders.filters("filter", QueryBuilders.boolQuery()))
            )
            .addAggregation(AggregationBuilders.min("min").field("constant"))
            .get();

        SearchResponse r2 = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(
                terms("terms1").collectMode(SubAggCollectionMode.DEPTH_FIRST)
                    .field("s_value")
                    .size(2)
                    .subAggregation(AggregationBuilders.filters("filter", QueryBuilders.boolQuery()))
            )
            .addAggregation(AggregationBuilders.min("min").field("constant"))
            .get();

        SearchResponse r3 = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(
                terms("terms1").collectMode(SubAggCollectionMode.BREADTH_FIRST)
                    .field("s_value")
                    .size(2)
                    .subAggregation(AggregationBuilders.filters("filter", QueryBuilders.boolQuery()))
            )
            .get();

        SearchResponse r4 = client().prepareSearch("idx")
            .setSize(0)
            .addAggregation(
                terms("terms1").collectMode(SubAggCollectionMode.DEPTH_FIRST)
                    .field("s_value")
                    .size(2)
                    .subAggregation(AggregationBuilders.filters("filter", QueryBuilders.boolQuery()))
            )
            .get();

        assertNotNull(r1.getAggregations().get("terms1"));
        assertNotNull(r2.getAggregations().get("terms1"));
        assertNotNull(r3.getAggregations().get("terms1"));
        assertNotNull(r4.getAggregations().get("terms1"));

        Terms terms = r1.getAggregations().get("terms1");
        Bucket b1 = terms.getBucketByKey("val0");
        InternalFilters f1 = b1.getAggregations().get("filter");
        long docCount1 = f1.getBuckets().get(0).getDocCount();
        Bucket b2 = terms.getBucketByKey("val1");
        InternalFilters f2 = b2.getAggregations().get("filter");
        long docCount2 = f1.getBuckets().get(0).getDocCount();

        for (SearchResponse response : new SearchResponse[] { r2, r3, r4 }) {
            terms = response.getAggregations().get("terms1");
            f1 = terms.getBucketByKey(b1.getKeyAsString()).getAggregations().get("filter");
            f2 = terms.getBucketByKey(b2.getKeyAsString()).getAggregations().get("filter");
            assertEquals(docCount1, f1.getBuckets().get(0).getDocCount());
            assertEquals(docCount2, f2.getBuckets().get(0).getDocCount());
        }
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=keyword")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("cache_test_idx").setId("1").setSource("s", "foo"),
            client().prepareIndex("cache_test_idx").setId("2").setSource("s", "bar")
        );

        // Make sure we are starting with a clear cache
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        SearchResponse r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                terms("terms").field("d")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "Math.random()", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        r = client().prepareSearch("cache_test_idx")
            .setSize(0)
            .addAggregation(
                terms("terms").field("d")
                    .script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "'foo_' + _value", Collections.emptyMap()))
            )
            .get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        r = client().prepareSearch("cache_test_idx").setSize(0).addAggregation(terms("terms").field("d")).get();
        assertSearchResponse(r);

        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getHitCount(),
            equalTo(0L)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareStats("cache_test_idx")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMissCount(),
            equalTo(2L)
        );
        internalCluster().wipeIndices("cache_test_idx");
    }

    public void testScriptWithValueType() throws Exception {
        SearchSourceBuilder builder = new SearchSourceBuilder().size(0)
            .aggregation(
                terms("terms").script(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "42", Collections.emptyMap()))
                    .userValueTypeHint(randomFrom(ValueType.NUMERIC, ValueType.NUMBER))
            );
        String source = builder.toString();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            SearchResponse response = client().prepareSearch("idx").setSource(SearchSourceBuilder.fromXContent(parser)).get();

            assertSearchResponse(response);
            Terms terms = response.getAggregations().get("terms");
            assertThat(terms, notNullValue());
            assertThat(terms.getName(), equalTo("terms"));
            assertThat(terms.getBuckets().size(), equalTo(1));
        }

        String invalidValueType = source.replaceAll("\"value_type\":\"n.*\"", "\"value_type\":\"foobar\"");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, invalidValueType)) {
            XContentParseException ex = expectThrows(
                XContentParseException.class,
                () -> client().prepareSearch("idx").setSource(SearchSourceBuilder.fromXContent(parser)).get()
            );
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(ex.getCause().getMessage(), containsString("Unknown value type [foobar]"));

        }

    }
}
