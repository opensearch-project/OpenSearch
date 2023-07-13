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
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestMultiSearchAction;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.StreamsUtils;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.opensearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchRequestTests extends OpenSearchTestCase {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MultiSearchRequestTests.class);

    public void testSimpleAdd() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/opensearch/action/search/simple-msearch1.json");
        assertThat(request.requests().size(), equalTo(8));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(1).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(false, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(2).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(2).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(false, true, true, false, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(3).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(3).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        assertThat(request.requests().get(4).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(4).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, false, false, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );

        assertThat(request.requests().get(5).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(6).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(6).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(7).indices(), is(Strings.EMPTY_ARRAY));
    }

    public void testFailWithUnknownKey() {
        final String requestContent = "{\"index\":\"test\", \"ignore_unavailable\" : true, \"unknown_key\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(restRequest, null, true)
        );
        assertEquals("key [unknown_key] is not supported in the metadata section", ex.getMessage());
    }

    public void testSimpleAddWithCarriageReturn() throws Exception {
        final String requestContent = "{\"index\":\"test\", \"ignore_unavailable\" : true, \"expand_wildcards\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
    }

    public void testCancelAfterIntervalAtParentAndFewChildRequest() throws Exception {
        final String requestContent = "{\"index\":\"test\", \"expand_wildcards\" : \"open,closed\", "
            + "\"cancel_after_time_interval\" : \"10s\"}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n {\"search_type\" : \"dfs_query_then_fetch\"}\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Collections.singletonMap("cancel_after_time_interval", "20s")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(2));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        // verifies that child search request parameter value is used for first search request
        assertEquals(new TimeValue(10, TimeUnit.SECONDS), request.requests().get(0).getCancelAfterTimeInterval());
        // verifies that parent msearch parameter value is used for second search request
        assertEquals(request.requests().get(1).searchType(), SearchType.DFS_QUERY_THEN_FETCH);
        assertEquals(new TimeValue(20, TimeUnit.SECONDS), request.requests().get(1).getCancelAfterTimeInterval());
    }

    public void testOnlyParentMSearchRequestWithCancelAfterTimeIntervalParameter() throws IOException {
        final String requestContent = "{\"index\":\"test\", \"expand_wildcards\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Collections.singletonMap("cancel_after_time_interval", "20s")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertEquals(new TimeValue(20, TimeUnit.SECONDS), request.requests().get(0).getCancelAfterTimeInterval());
    }

    public void testDefaultIndicesOptions() throws IOException {
        final String requestContent = "{\"index\":\"test\", \"expand_wildcards\" : \"open,closed\"}}\r\n"
            + "{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).withParams(Collections.singletonMap("ignore_unavailable", "true")).build();
        MultiSearchRequest request = RestMultiSearchAction.parseRequest(restRequest, null, true);
        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(
            request.requests().get(0).indicesOptions(),
            equalTo(IndicesOptions.fromOptions(true, true, true, true, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
    }

    public void testSimpleAdd2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/opensearch/action/search/simple-msearch2.json");
        assertThat(request.requests().size(), equalTo(5));
        assertThat(request.requests().get(0).indices()[0], equalTo("test"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test"));
        assertThat(request.requests().get(2).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
        assertThat(request.requests().get(4).indices(), is(Strings.EMPTY_ARRAY));
    }

    public void testSimpleAdd3() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/opensearch/action/search/simple-msearch3.json");
        assertThat(request.requests().size(), equalTo(4));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(3).indices(), is(Strings.EMPTY_ARRAY));
        assertThat(request.requests().get(3).searchType(), equalTo(SearchType.DFS_QUERY_THEN_FETCH));
    }

    public void testSimpleAdd4() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromFile("/org/opensearch/action/search/simple-msearch4.json");
        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).requestCache(), equalTo(true));
        assertThat(request.requests().get(0).preference(), nullValue());
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).requestCache(), nullValue());
        assertThat(request.requests().get(1).preference(), equalTo("_local"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).routing(), equalTo("123"));
    }

    public void testEmptyFirstLine1() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromString(
            "\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
        );
        assertThat(request.requests().size(), equalTo(4));
        for (SearchRequest searchRequest : request.requests()) {
            assertThat(searchRequest.indices().length, equalTo(0));
            assertThat(searchRequest.source().query(), instanceOf(MatchAllQueryBuilder.class));
        }
        assertWarnings(
            "support for empty first line before any action metadata in msearch API is deprecated and will be removed "
                + "in the next major version"
        );
    }

    public void testEmptyFirstLine2() throws Exception {
        MultiSearchRequest request = parseMultiSearchRequestFromString(
            "\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "{}\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
                + "\n"
                + "{ \"query\": {\"match_all\": {}}}\n"
        );
        assertThat(request.requests().size(), equalTo(4));
        for (SearchRequest searchRequest : request.requests()) {
            assertThat(searchRequest.indices().length, equalTo(0));
            assertThat(searchRequest.source().query(), instanceOf(MatchAllQueryBuilder.class));
        }
        assertWarnings(
            "support for empty first line before any action metadata in msearch API is deprecated and will be removed "
                + "in the next major version"
        );
    }

    public void testResponseErrorToXContent() {
        long tookInMillis = randomIntBetween(1, 1000);
        MultiSearchResponse response = new MultiSearchResponse(
            new MultiSearchResponse.Item[] {
                new MultiSearchResponse.Item(null, new IllegalStateException("foobar")),
                new MultiSearchResponse.Item(null, new IllegalStateException("baaaaaazzzz")) },
            tookInMillis
        );

        assertEquals(
            "{\"took\":"
                + tookInMillis
                + ",\"responses\":["
                + "{"
                + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"}],"
                + "\"type\":\"illegal_state_exception\",\"reason\":\"foobar\"},\"status\":500"
                + "},"
                + "{"
                + "\"error\":{\"root_cause\":[{\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"}],"
                + "\"type\":\"illegal_state_exception\",\"reason\":\"baaaaaazzzz\"},\"status\":500"
                + "}"
                + "]}",
            Strings.toString(XContentType.JSON, response)
        );
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchRequest request = new MultiSearchRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    public void testMsearchTerminatedByNewline() throws Exception {
        String mserchAction = StreamsUtils.copyToStringFromClasspath("/org/opensearch/action/search/simple-msearch5.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(mserchAction.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        IllegalArgumentException expectThrows = expectThrows(
            IllegalArgumentException.class,
            () -> RestMultiSearchAction.parseRequest(restRequest, null, true)
        );
        assertEquals("The msearch request must be terminated by a newline [\n]", expectThrows.getMessage());

        String mserchActionWithNewLine = mserchAction + "\n";
        RestRequest restRequestWithNewLine = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(mserchActionWithNewLine.getBytes(StandardCharsets.UTF_8)),
            XContentType.JSON
        ).build();
        MultiSearchRequest msearchRequest = RestMultiSearchAction.parseRequest(restRequestWithNewLine, null, true);
        assertEquals(3, msearchRequest.requests().size());
    }

    private MultiSearchRequest parseMultiSearchRequestFromString(String request) throws IOException {
        return parseMultiSearchRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(request), XContentType.JSON).build()
        );
    }

    private MultiSearchRequest parseMultiSearchRequestFromFile(String sample) throws IOException {
        byte[] data = StreamsUtils.copyToBytesFromClasspath(sample);
        return parseMultiSearchRequest(
            new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), XContentType.JSON).build()
        );

    }

    private MultiSearchRequest parseMultiSearchRequest(RestRequest restRequest) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        RestMultiSearchAction.parseMultiLineRequest(restRequest, SearchRequest.DEFAULT_INDICES_OPTIONS, true, (searchRequest, parser) -> {
            searchRequest.source(SearchSourceBuilder.fromXContent(parser, false));
            request.add(searchRequest);
        });
        return request;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            singletonList(
                new NamedXContentRegistry.Entry(
                    QueryBuilder.class,
                    new ParseField(MatchAllQueryBuilder.NAME),
                    (p, c) -> MatchAllQueryBuilder.fromXContent(p)
                )
            )
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(
            singletonList(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
        );
    }

    public void testMultiLineSerialization() throws IOException {
        int iters = 16;
        for (int i = 0; i < iters; i++) {
            // The only formats that support stream separator
            XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE);
            MultiSearchRequest originalRequest = createMultiSearchRequest();

            byte[] originalBytes = MultiSearchRequest.writeMultiLineFormat(originalRequest, xContentType.xContent());
            MultiSearchRequest parsedRequest = new MultiSearchRequest();
            CheckedBiConsumer<SearchRequest, XContentParser, IOException> consumer = (r, p) -> {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(p, false);
                if (searchSourceBuilder.equals(new SearchSourceBuilder()) == false) {
                    r.source(searchSourceBuilder);
                }
                parsedRequest.add(r);
            };
            MultiSearchRequest.readMultiLineFormat(
                new BytesArray(originalBytes),
                xContentType.xContent(),
                consumer,
                null,
                null,
                null,
                null,
                null,
                xContentRegistry(),
                true,
                deprecationLogger
            );
            assertEquals(originalRequest, parsedRequest);
        }
    }

    public void testSerDeWithCancelAfterTimeIntervalParameterAndRandomVersion() throws IOException {
        final String requestContent = "{\"index\":\"test\", \"expand_wildcards\" : \"open,closed\", "
            + "\"cancel_after_time_interval\" : \"10s\"}\r\n{\"query\" : {\"match_all\" :{}}}\r\n";
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(requestContent),
            XContentType.JSON
        ).build();
        Version version = VersionUtils.randomVersion(random());
        MultiSearchRequest originalRequest = RestMultiSearchAction.parseRequest(restRequest, null, true);
        MultiSearchRequest deserializedRequest = copyWriteable(originalRequest, writableRegistry(), MultiSearchRequest::new, version);
        assertEquals(
            originalRequest.requests().get(0).getCancelAfterTimeInterval(),
            deserializedRequest.requests().get(0).getCancelAfterTimeInterval()
        );
    }

    public void testWritingExpandWildcards() throws IOException {
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                true,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "all"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                true,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open,closed"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open,hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                true,
                false,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "open"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                true,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "closed,hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                true,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "closed"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                false,
                true,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "hidden"
        );
        assertExpandWildcardsValue(
            IndicesOptions.fromOptions(
                randomBoolean(),
                randomBoolean(),
                false,
                false,
                false,
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                randomBoolean()
            ),
            "none"
        );
    }

    private void assertExpandWildcardsValue(IndicesOptions options, String expectedValue) throws IOException {
        SearchRequest request = new SearchRequest();
        request.indicesOptions(options);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            MultiSearchRequest.writeSearchRequestParams(request, builder);
            Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                BytesReference.bytes(builder).streamInput(),
                false
            );
            final String value = (String) map.get("expand_wildcards");
            assertEquals(expectedValue, value);
        }
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createMultiSearchRequest(), MultiSearchRequestTests::copyRequest, MultiSearchRequestTests::mutate);
    }

    private static MultiSearchRequest mutate(MultiSearchRequest searchRequest) throws IOException {
        MultiSearchRequest mutation = copyRequest(searchRequest);
        List<CheckedRunnable<IOException>> mutators = new ArrayList<>();
        mutators.add(
            () -> mutation.indicesOptions(
                randomValueOtherThan(
                    searchRequest.indicesOptions(),
                    () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
                )
            )
        );
        mutators.add(() -> mutation.maxConcurrentSearchRequests(randomIntBetween(1, 32)));
        mutators.add(() -> mutation.add(createSimpleSearchRequest()));
        randomFrom(mutators).run();
        return mutation;
    }

    private static MultiSearchRequest copyRequest(MultiSearchRequest request) {
        MultiSearchRequest copy = new MultiSearchRequest();
        if (request.maxConcurrentSearchRequests() > 0) {
            copy.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }
        copy.indicesOptions(request.indicesOptions());
        for (SearchRequest searchRequest : request.requests()) {
            copy.add(searchRequest);
        }
        return copy;
    }

    private static MultiSearchRequest createMultiSearchRequest() {
        int numSearchRequest = randomIntBetween(1, 128);
        MultiSearchRequest request = new MultiSearchRequest();
        for (int j = 0; j < numSearchRequest; j++) {
            SearchRequest searchRequest = createSimpleSearchRequest();

            if (randomBoolean()) {
                searchRequest.allowPartialSearchResults(true);
            }

            // scroll is not supported in the current msearch api, so unset it:
            searchRequest.scroll((Scroll) null);

            // only expand_wildcards, ignore_unavailable and allow_no_indices can be specified from msearch api, so unset other options:
            IndicesOptions randomlyGenerated = searchRequest.indicesOptions();
            IndicesOptions msearchDefault = SearchRequest.DEFAULT_INDICES_OPTIONS;
            searchRequest.indicesOptions(
                IndicesOptions.fromOptions(
                    randomlyGenerated.ignoreUnavailable(),
                    randomlyGenerated.allowNoIndices(),
                    randomlyGenerated.expandWildcardsOpen(),
                    randomlyGenerated.expandWildcardsClosed(),
                    msearchDefault.expandWildcardsHidden(),
                    msearchDefault.allowAliasesToMultipleIndices(),
                    msearchDefault.forbidClosedIndices(),
                    msearchDefault.ignoreAliases(),
                    msearchDefault.ignoreThrottled()
                )
            );

            request.add(searchRequest);
        }
        return request;
    }

    private static SearchRequest createSimpleSearchRequest() {
        return randomSearchRequest(() -> {
            // No need to return a very complex SearchSourceBuilder here, that is tested elsewhere
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(randomInt(10));
            searchSourceBuilder.size(randomIntBetween(20, 100));
            return searchSourceBuilder;
        });
    }

}
