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

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.ArrayUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.geometry.LinearRing;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.AbstractSearchTestCase;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntConsumer;

import static java.util.Collections.emptyMap;
import static org.opensearch.action.search.SearchType.DFS_QUERY_THEN_FETCH;
import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class SearchRequestTests extends AbstractSearchTestCase {

    @Override
    protected SearchRequest createSearchRequest() throws IOException {
        SearchRequest request = super.createSearchRequest();
        if (randomBoolean()) {
            return request;
        }
        // clusterAlias and absoluteStartMillis do not have public getters/setters hence we randomize them only in this test specifically.
        return SearchRequest.subSearchRequest(
            request,
            request.indices(),
            randomAlphaOfLengthBetween(5, 10),
            randomNonNegativeLong(),
            randomBoolean()
        );
    }

    public void testClone() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchRequest clonedRequest = searchRequest.deepCopy();
        assertEquals(searchRequest.hashCode(), clonedRequest.hashCode());
        assertNotSame(searchRequest, clonedRequest);

        String[] includes = new String[] { "field1.*" };
        String[] excludes = new String[] { "field2.*" };
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        SearchSourceBuilder source = new SearchSourceBuilder().fetchSource(fetchSourceContext);
        SearchRequest complexSearchRequest = createSearchRequest().source(source);
        complexSearchRequest.requestCache(false);
        complexSearchRequest.scroll(new TimeValue(1000));
        SearchRequest clonedComplexRequest = complexSearchRequest.deepCopy();
        assertEquals(complexSearchRequest.hashCode(), clonedComplexRequest.hashCode());
        assertNotSame(complexSearchRequest, clonedComplexRequest);
        assertEquals(fetchSourceContext, clonedComplexRequest.source().fetchSource());
        assertNotSame(fetchSourceContext, clonedComplexRequest.source().fetchSource());
        // Change the value of the original includes array and excludes array
        includes[0] = "new_field1.*";
        excludes[0] = "new_field2.*";
        // Values in the original fetchSource object should be updated
        assertEquals("new_field1.*", complexSearchRequest.source().fetchSource().includes()[0]);
        assertEquals("new_field2.*", complexSearchRequest.source().fetchSource().excludes()[0]);
        // Values in the cloned fetchSource object should not be updated
        assertEquals("field1.*", clonedComplexRequest.source().fetchSource().includes()[0]);
        assertEquals("field2.*", clonedComplexRequest.source().fetchSource().excludes()[0]);
    }

    public void testWithLocalReduction() {
        expectThrows(NullPointerException.class, () -> SearchRequest.subSearchRequest(null, Strings.EMPTY_ARRAY, "", 0, randomBoolean()));
        SearchRequest request = new SearchRequest();
        expectThrows(NullPointerException.class, () -> SearchRequest.subSearchRequest(request, null, "", 0, randomBoolean()));
        expectThrows(
            NullPointerException.class,
            () -> SearchRequest.subSearchRequest(request, new String[] { null }, "", 0, randomBoolean())
        );
        expectThrows(
            NullPointerException.class,
            () -> SearchRequest.subSearchRequest(request, Strings.EMPTY_ARRAY, null, 0, randomBoolean())
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> SearchRequest.subSearchRequest(request, Strings.EMPTY_ARRAY, "", -1, randomBoolean())
        );
        SearchRequest searchRequest = SearchRequest.subSearchRequest(request, Strings.EMPTY_ARRAY, "", 0, randomBoolean());
        assertNull(searchRequest.validate());
    }

    public void testSerialization() throws Exception {
        SearchRequest searchRequest = createSearchRequest();
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new);
        assertEquals(deserializedRequest, searchRequest);
        assertEquals(deserializedRequest.hashCode(), searchRequest.hashCode());
        assertNotSame(deserializedRequest, searchRequest);
    }

    public void testRandomVersionSerialization() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        Version version = VersionUtils.randomVersion(random());
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new, version);
        assertEquals(searchRequest.isCcsMinimizeRoundtrips(), deserializedRequest.isCcsMinimizeRoundtrips());
        assertEquals(searchRequest.getLocalClusterAlias(), deserializedRequest.getLocalClusterAlias());
        assertEquals(searchRequest.getAbsoluteStartMillis(), deserializedRequest.getAbsoluteStartMillis());
        assertEquals(searchRequest.isFinalReduce(), deserializedRequest.isFinalReduce());
        assertEquals(searchRequest.getCancelAfterTimeInterval(), deserializedRequest.getCancelAfterTimeInterval());
    }

    public void testIllegalArguments() {
        SearchRequest searchRequest = new SearchRequest();
        assertNotNull(searchRequest.indices());
        assertNotNull(searchRequest.indicesOptions());
        assertNotNull(searchRequest.searchType());

        NullPointerException e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String[]) null));
        assertEquals("indices must not be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> searchRequest.indices((String) null));
        assertEquals("index must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.indicesOptions(null));
        assertEquals("indicesOptions must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.searchType((SearchType) null));
        assertEquals("searchType must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.source(null));
        assertEquals("source must not be null", e.getMessage());

        e = expectThrows(NullPointerException.class, () -> searchRequest.scroll((TimeValue) null));
        assertEquals("keepAlive must not be null", e.getMessage());
    }

    public void testValidate() throws IOException {
        {
            // if scroll isn't set, validate should never add errors
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.scroll((Scroll) null);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNull(validationErrors);
        }
        {
            // disabling `track_total_hits` isn't valid in scroll context
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            // make sure we don't set the request cache for a scroll query
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().trackTotalHits(false);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("disabling [track_total_hits] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // scroll and `from` isn't valid
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            // make sure we don't set the request cache for a scroll query
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            searchRequest.source().from(10);
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [from] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // scroll and `size` is `0`
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder().size(0));
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("[size] cannot be [0] in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // Rescore is not allowed on scroll requests
            SearchRequest searchRequest = createSearchRequest().source(new SearchSourceBuilder());
            searchRequest.source().addRescorer(new QueryRescorerBuilder(QueryBuilders.matchAllQuery()));
            searchRequest.requestCache(false);
            searchRequest.scroll(new TimeValue(1000));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [rescore] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
        {
            // Reader context with scroll
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder("id"))
            ).scroll(TimeValue.timeValueMillis(randomIntBetween(1, 100)));
            ActionRequestValidationException validationErrors = searchRequest.validate();
            assertNotNull(validationErrors);
            assertEquals(1, validationErrors.validationErrors().size());
            assertEquals("using [point in time] is not allowed in a scroll context", validationErrors.validationErrors().get(0));
        }
    }

    public void testCopyConstructor() throws IOException {
        SearchRequest searchRequest = createSearchRequest();
        SearchRequest deserializedRequest = copyWriteable(searchRequest, namedWriteableRegistry, SearchRequest::new);
        assertEquals(deserializedRequest, searchRequest);
        assertEquals(deserializedRequest.hashCode(), searchRequest.hashCode());
        assertNotSame(deserializedRequest, searchRequest);
    }

    public void testParseSearchRequest() throws IOException {
        RestRequest restRequest = new FakeRestRequest();
        SearchRequest searchRequest = createSearchRequest();
        IntConsumer setSize = mock(IntConsumer.class);

        restRequest.params().put("index", "index1,index2");
        restRequest.params().put("batched_reduce_size", "512");
        restRequest.params().put("pre_filter_shard_size", "128");
        restRequest.params().put("max_concurrent_shard_requests", "10");
        restRequest.params().put("allow_partial_search_results", "true");
        restRequest.params().put("phase_took", "false");
        restRequest.params().put("search_type", "dfs_query_then_fetch");
        restRequest.params().put("request_cache", "true");
        restRequest.params().put("scroll", "1m");
        restRequest.params().put("routing", "routing_value");
        restRequest.params().put("preference", "preference_value");
        restRequest.params().put("search_pipeline", "pipeline_value");
        restRequest.params().put("ccs_minimize_roundtrips", "true");
        restRequest.params().put("cancel_after_time_interval", "5s");

        RestSearchAction.parseSearchRequest(searchRequest, restRequest, null, namedWriteableRegistry, setSize);

        assertEquals(Arrays.asList("index1", "index2"), Arrays.asList(searchRequest.indices()));
        assertEquals(512, searchRequest.getBatchedReduceSize());
        assertEquals(Integer.valueOf(128), searchRequest.getPreFilterShardSize());
        assertEquals(10, searchRequest.getMaxConcurrentShardRequests());
        assertTrue(searchRequest.allowPartialSearchResults());
        assertFalse(searchRequest.isPhaseTook());
        assertEquals(DFS_QUERY_THEN_FETCH, searchRequest.searchType());
        assertEquals(true, searchRequest.requestCache());
        assertEquals(TimeValue.timeValueMinutes(1), searchRequest.scroll().keepAlive());
        assertEquals("routing_value", searchRequest.routing());
        assertEquals("preference_value", searchRequest.preference());
        assertEquals("pipeline_value", searchRequest.pipeline());
        assertTrue(searchRequest.isCcsMinimizeRoundtrips());
        assertEquals(TimeValue.timeValueSeconds(5), searchRequest.getCancelAfterTimeInterval());
    }

    public void testParseSearchRequestWithUnsupportedSearchType() throws IOException {
        RestRequest restRequest = new FakeRestRequest();
        SearchRequest searchRequest = createSearchRequest();
        IntConsumer setSize = mock(IntConsumer.class);
        restRequest.params().put("search_type", "query_and_fetch");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RestSearchAction.parseSearchRequest(searchRequest, restRequest, null, namedWriteableRegistry, setSize)
        );
        assertEquals("Unsupported search type [query_and_fetch]", exception.getMessage());
    }

    public void testEqualsAndHashcode() throws IOException {
        checkEqualsAndHashCode(createSearchRequest(), SearchRequest::new, this::mutate);
    }

    private SearchRequest mutate(SearchRequest searchRequest) {
        SearchRequest mutation = new SearchRequest(searchRequest);
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> mutation.indices(ArrayUtils.concat(searchRequest.indices(), new String[] { randomAlphaOfLength(10) })));
        mutators.add(
            () -> mutation.indicesOptions(
                randomValueOtherThan(
                    searchRequest.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                )
            )
        );
        mutators.add(() -> mutation.preference(randomValueOtherThan(searchRequest.preference(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.routing(randomValueOtherThan(searchRequest.routing(), () -> randomAlphaOfLengthBetween(3, 10))));
        mutators.add(() -> mutation.requestCache((randomValueOtherThan(searchRequest.requestCache(), OpenSearchTestCase::randomBoolean))));
        mutators.add(
            () -> mutation.scroll(
                randomValueOtherThan(searchRequest.scroll(), () -> new Scroll(new TimeValue(randomNonNegativeLong() % 100000)))
            )
        );
        mutators.add(
            () -> mutation.searchType(
                randomValueOtherThan(searchRequest.searchType(), () -> randomFrom(DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH))
            )
        );
        mutators.add(() -> mutation.source(randomValueOtherThan(searchRequest.source(), this::createSearchSourceBuilder)));
        mutators.add(() -> mutation.setCcsMinimizeRoundtrips(searchRequest.isCcsMinimizeRoundtrips() == false));
        mutators.add(
            () -> mutation.setPhaseTook(searchRequest.isPhaseTook() == null ? randomBoolean() : searchRequest.isPhaseTook() == false)
        );
        mutators.add(
            () -> mutation.setCancelAfterTimeInterval(
                searchRequest.getCancelAfterTimeInterval() != null
                    ? null
                    : TimeValue.parseTimeValue(randomTimeValue(), null, "cancel_after_time_interval")
            )
        );
        randomFrom(mutators).run();
        return mutation;
    }

    public void testDescriptionForDefault() {
        assertThat(toDescription(new SearchRequest()), equalTo("indices[], search_type[QUERY_THEN_FETCH], source[]"));
    }

    public void testDescriptionIncludesScroll() {
        assertThat(
            toDescription(new SearchRequest().scroll(TimeValue.timeValueMinutes(5))),
            equalTo("indices[], search_type[QUERY_THEN_FETCH], scroll[5m], source[]")
        );
    }

    public void testDescriptionOnSourceError() {
        LinearRing linearRing = new LinearRing(new double[] { -25, -35, -25 }, new double[] { -25, -35, -25 });
        GeoShapeQueryBuilder queryBuilder = new GeoShapeQueryBuilder("geo", linearRing);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().query(queryBuilder));
        assertThat(
            toDescription(request),
            equalTo(
                "indices[], search_type[QUERY_THEN_FETCH], source[<error: java.lang.UnsupportedOperationException: line ring cannot be serialized using GeoJson>]"
            )
        );
    }

    private String toDescription(SearchRequest request) {
        return request.createTask(0, "test", SearchAction.NAME, TaskId.EMPTY_TASK_ID, emptyMap()).getDescription();
    }
}
