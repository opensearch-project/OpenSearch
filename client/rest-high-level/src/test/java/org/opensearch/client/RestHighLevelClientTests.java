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

package org.opensearch.client;

import com.fasterxml.jackson.core.JsonParseException;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.core.MainRequest;
import org.opensearch.client.core.MainResponse;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.cbor.CborXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.index.rankeval.DiscountedCumulativeGain;
import org.opensearch.index.rankeval.EvaluationMetric;
import org.opensearch.index.rankeval.ExpectedReciprocalRank;
import org.opensearch.index.rankeval.MeanReciprocalRank;
import org.opensearch.index.rankeval.MetricDetail;
import org.opensearch.index.rankeval.PrecisionAtK;
import org.opensearch.index.rankeval.RecallAtK;
import org.opensearch.join.aggregations.ChildrenAggregationBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.rest.yaml.restspec.ClientYamlSuiteRestApi;
import org.opensearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ProtocolVersion;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends OpenSearchTestCase {

    private static final String SUBMIT_TASK_PREFIX = "submit_";
    private static final String SUBMIT_TASK_SUFFIX = "_task";
    private static final ProtocolVersion HTTP_PROTOCOL = new ProtocolVersion("http", 1, 1);
    private static final RequestLine REQUEST_LINE = new RequestLine(HttpGet.METHOD_NAME, "/", HTTP_PROTOCOL);

    /**
     * These APIs do not use a Request object (because they don't have a body, or any request parameters).
     * The method naming/parameter assertions use this {@code Set} to determine which rules to apply.
     * (This is also used for async variants of these APIs when they exist)
     */
    private static final Set<String> APIS_WITHOUT_REQUEST_OBJECT = Sets.newHashSet(
        // core
        "ping",
        "info",
        "delete_all_pits",
        "get_all_pits",
        // security
        "security.get_ssl_certificates",
        "security.authenticate",
        "security.get_user_privileges",
        "security.get_builtin_privileges",
        // license
        "license.get_trial_status",
        "license.get_basic_status"

    );

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient, RestClient::close, Collections.emptyList());
    }

    public void testCloseIsIdempotent() throws IOException {
        restHighLevelClient.close();
        verify(restClient, times(1)).close();
        restHighLevelClient.close();
        verify(restClient, times(2)).close();
        restHighLevelClient.close();
        verify(restClient, times(3)).close();
    }

    public void testPingSuccessful() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.OK));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertTrue(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPing404NotFound() throws IOException {
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(RestStatus.NOT_FOUND));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
        assertFalse(restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testPingSocketTimeout() throws IOException {
        when(restClient.performRequest(any(Request.class))).thenThrow(new SocketTimeoutException());
        expectThrows(SocketTimeoutException.class, () -> restHighLevelClient.ping(RequestOptions.DEFAULT));
    }

    public void testInfo() throws IOException {
        MainResponse testInfo = new MainResponse(
            "nodeName",
            new MainResponse.Version(
                "number",
                "buildType",
                "buildHash",
                "buildDate",
                true,
                "luceneVersion",
                "minimumWireCompatibilityVersion",
                "minimumIndexCompatibilityVersion"
            ),
            "clusterName",
            "clusterUuid"
        );
        mockResponse((ToXContentFragment) (builder, params) -> {
            // taken from the server side MainResponse
            builder.field("name", testInfo.getNodeName());
            builder.field("cluster_name", testInfo.getClusterName());
            builder.field("cluster_uuid", testInfo.getClusterUuid());
            builder.startObject("version")
                .field("number", testInfo.getVersion().getNumber())
                .field("build_type", testInfo.getVersion().getBuildType())
                .field("build_hash", testInfo.getVersion().getBuildHash())
                .field("build_date", testInfo.getVersion().getBuildDate())
                .field("build_snapshot", testInfo.getVersion().isSnapshot())
                .field("lucene_version", testInfo.getVersion().getLuceneVersion())
                .field("minimum_wire_compatibility_version", testInfo.getVersion().getMinimumWireCompatibilityVersion())
                .field("minimum_index_compatibility_version", testInfo.getVersion().getMinimumIndexCompatibilityVersion())
                .endObject();
            return builder;
        });
        MainResponse receivedInfo = restHighLevelClient.info(RequestOptions.DEFAULT);
        assertEquals(testInfo, receivedInfo);
    }

    public void testSearchScroll() throws IOException {
        SearchResponse mockSearchResponse = new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), InternalAggregations.EMPTY, null, false, false, null, 1),
            randomAlphaOfLengthBetween(5, 10),
            5,
            5,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        mockResponse(mockSearchResponse);
        SearchResponse searchResponse = restHighLevelClient.scroll(
            new SearchScrollRequest(randomAlphaOfLengthBetween(5, 10)),
            RequestOptions.DEFAULT
        );
        assertEquals(mockSearchResponse.getScrollId(), searchResponse.getScrollId());
        assertEquals(0, searchResponse.getHits().getTotalHits().value);
        assertEquals(5, searchResponse.getTotalShards());
        assertEquals(5, searchResponse.getSuccessfulShards());
        assertEquals(100, searchResponse.getTook().getMillis());
    }

    public void testClearScroll() throws IOException {
        ClearScrollResponse mockClearScrollResponse = new ClearScrollResponse(randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE));
        mockResponse(mockClearScrollResponse);
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(randomAlphaOfLengthBetween(5, 10));
        ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        assertEquals(mockClearScrollResponse.isSucceeded(), clearScrollResponse.isSucceeded());
        assertEquals(mockClearScrollResponse.getNumFreed(), clearScrollResponse.getNumFreed());
    }

    private void mockResponse(ToXContent toXContent) throws IOException {
        Response response = mock(Response.class);
        ContentType contentType = ContentType.parse(RequestConverters.REQUEST_BODY_CONTENT_TYPE.mediaType());
        String requestBody = toXContent(toXContent, RequestConverters.REQUEST_BODY_CONTENT_TYPE, false).utf8ToString();
        when(response.getEntity()).thenReturn(new StringEntity(requestBody, contentType));
        when(restClient.performRequest(any(Request.class))).thenReturn(response);
    }

    public void testRequestValidation() {
        ActionRequestValidationException validationException = new ActionRequestValidationException();
        validationException.addValidationError("validation error");
        ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return validationException;
            }
        };

        {
            ActionRequestValidationException actualException = expectThrows(
                ActionRequestValidationException.class,
                () -> restHighLevelClient.performRequest(request, null, RequestOptions.DEFAULT, null, null)
            );
            assertSame(validationException, actualException);
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            restHighLevelClient.performRequestAsync(request, null, RequestOptions.DEFAULT, null, trackingActionListener, null);
            assertSame(validationException, trackingActionListener.exception.get());
        }
    }

    public void testNullableActionListener() {
        ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };

        assertThrows(
            IllegalArgumentException.class,
            () -> restHighLevelClient.performRequestAsync(request, null, RequestOptions.DEFAULT, null, null, null)
        );
    }

    public void testParseEntity() throws IOException {
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> restHighLevelClient.parseEntity(null, null));
            assertEquals("Response body expected but not returned", ise.getMessage());
        }
        {
            IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> restHighLevelClient.parseEntity(new StringEntity("", (ContentType) null), null)
            );
            assertEquals("OpenSearch didn't return the [Content-Type] header, unable to parse response body", ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("", ContentType.APPLICATION_SVG_XML);
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> restHighLevelClient.parseEntity(entity, null));
            assertEquals("Unsupported Content-Type: " + entity.getContentType(), ise.getMessage());
        }
        {
            CheckedFunction<XContentParser, String, IOException> entityParser = parser -> {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                String value = parser.text();
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                return value;
            };
            HttpEntity jsonEntity = new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
            assertEquals("value", restHighLevelClient.parseEntity(jsonEntity, entityParser));
            HttpEntity yamlEntity = new StringEntity("---\nfield: value\n", ContentType.create("application/yaml"));
            assertEquals("value", restHighLevelClient.parseEntity(yamlEntity, entityParser));
            HttpEntity smileEntity = createBinaryEntity(SmileXContent.contentBuilder(), ContentType.create("application/smile"));
            assertEquals("value", restHighLevelClient.parseEntity(smileEntity, entityParser));
            HttpEntity cborEntity = createBinaryEntity(CborXContent.contentBuilder(), ContentType.create("application/cbor"));
            assertEquals("value", restHighLevelClient.parseEntity(cborEntity, entityParser));
        }
    }

    private static HttpEntity createBinaryEntity(XContentBuilder xContentBuilder, ContentType contentType) throws IOException {
        try (XContentBuilder builder = xContentBuilder) {
            builder.startObject();
            builder.field("field", "value");
            builder.endObject();
            return new ByteArrayEntity(BytesReference.bytes(builder).toBytesRef().bytes, contentType);
        }
    }

    public void testConvertExistsResponse() {
        RestStatus restStatus = randomBoolean() ? RestStatus.OK : randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        boolean result = RestHighLevelClient.convertExistsResponse(response);
        assertEquals(restStatus == RestStatus.OK, result);
    }

    public void testParseResponseException() throws IOException {
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            OpenSearchException openSearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals(responseException.getMessage(), openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getCause());
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            httpResponse.setEntity(
                new StringEntity(
                    "{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                    ContentType.APPLICATION_JSON
                )
            );
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            OpenSearchException openSearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("OpenSearch exception [type=exception, reason=test error message]", openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getSuppressed()[0]);
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            OpenSearchException openSearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("Unable to parse response body", openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getCause());
            assertThat(openSearchException.getSuppressed()[0], instanceOf(IOException.class));
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            OpenSearchException openSearchException = restHighLevelClient.parseResponseException(responseException);
            assertEquals("Unable to parse response body", openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getCause());
            assertThat(openSearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
        }
    }

    public void testPerformRequestOnSuccess() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        when(restClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        {
            Integer result = restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.emptySet()
            );
            assertEquals(restStatus.getStatus(), result.intValue());
        }
        {
            IOException ioe = expectThrows(
                IOException.class,
                () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT, response -> {
                    throw new IllegalStateException();
                }, Collections.emptySet())
            );
            assertEquals(
                "Unable to parse response body for Response{requestLine=GET / http/1.1, host=http://localhost:9200, "
                    + "response=HTTP/1.1 "
                    + restStatus.getStatus()
                    + " "
                    + restStatus.name()
                    + "}",
                ioe.getMessage()
            );
        }
    }

    public void testPerformRequestOnResponseExceptionWithoutEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.emptySet()
            )
        );
        assertEquals(responseException.getMessage(), openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
    }

    public void testPerformRequestOnResponseExceptionWithEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        httpResponse.setEntity(
            new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON)
        );
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.emptySet()
            )
        );
        assertEquals("OpenSearch exception [type=exception, reason=test error message]", openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getSuppressed()[0]);
    }

    public void testPerformRequestOnResponseExceptionWithBrokenEntity() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.emptySet()
            )
        );
        assertEquals("Unable to parse response body", openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
        assertThat(openSearchException.getSuppressed()[0], instanceOf(JsonParseException.class));
    }

    public void testPerformRequestOnResponseExceptionWithBrokenEntity2() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.emptySet()
            )
        );
        assertEquals("Unable to parse response body", openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
        assertThat(openSearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
    }

    public void testPerformRequestOnResponseExceptionWithIgnores() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        // although we got an exception, we turn it into a successful response because the status code was provided among ignores
        assertEquals(
            Integer.valueOf(404),
            restHighLevelClient.performRequest(
                mainRequest,
                requestConverter,
                RequestOptions.DEFAULT,
                response -> response.getStatusLine().getStatusCode(),
                Collections.singleton(404)
            )
        );
    }

    public void testPerformRequestOnResponseExceptionWithIgnoresErrorNoBody() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT, response -> {
                throw new IllegalStateException();
            }, Collections.singleton(404))
        );
        assertEquals(RestStatus.NOT_FOUND, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
        assertEquals(responseException.getMessage(), openSearchException.getMessage());
    }

    public void testPerformRequestOnResponseExceptionWithIgnoresErrorValidBody() throws IOException {
        MainRequest mainRequest = new MainRequest();
        CheckedFunction<MainRequest, Request, IOException> requestConverter = request -> new Request(HttpGet.METHOD_NAME, "/");
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":404}", ContentType.APPLICATION_JSON));
        Response mockResponse = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(mockResponse);
        when(restClient.performRequest(any(Request.class))).thenThrow(responseException);
        OpenSearchException openSearchException = expectThrows(
            OpenSearchException.class,
            () -> restHighLevelClient.performRequest(mainRequest, requestConverter, RequestOptions.DEFAULT, response -> {
                throw new IllegalStateException();
            }, Collections.singleton(404))
        );
        assertEquals(RestStatus.NOT_FOUND, openSearchException.status());
        assertSame(responseException, openSearchException.getSuppressed()[0]);
        assertEquals("OpenSearch exception [type=exception, reason=test error message]", openSearchException.getMessage());
    }

    public void testWrapResponseListenerOnSuccess() {
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(),
                trackingActionListener,
                Collections.emptySet()
            );
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            responseListener.onSuccess(new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse));
            assertNull(trackingActionListener.exception.get());
            assertEquals(restStatus.getStatus(), trackingActionListener.statusCode.get());
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> { throw new IllegalStateException(); },
                trackingActionListener,
                Collections.emptySet()
            );
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            responseListener.onSuccess(new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse));
            assertThat(trackingActionListener.exception.get(), instanceOf(IOException.class));
            IOException ioe = (IOException) trackingActionListener.exception.get();
            assertEquals(
                "Unable to parse response body for Response{requestLine=GET / http/1.1, host=http://localhost:9200, "
                    + "response=HTTP/1.1 "
                    + restStatus.getStatus()
                    + " "
                    + restStatus.name()
                    + "}",
                ioe.getMessage()
            );
            assertThat(ioe.getCause(), instanceOf(IllegalStateException.class));
        }
    }

    public void testWrapResponseListenerOnException() {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> response.getStatusLine().getStatusCode(),
            trackingActionListener,
            Collections.emptySet()
        );
        IllegalStateException exception = new IllegalStateException();
        responseListener.onFailure(exception);
        assertSame(exception, trackingActionListener.exception.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithoutEntity() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> response.getStatusLine().getStatusCode(),
            trackingActionListener,
            Collections.emptySet()
        );
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
        OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
        assertEquals(responseException.getMessage(), openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
    }

    public void testWrapResponseListenerOnResponseExceptionWithEntity() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> response.getStatusLine().getStatusCode(),
            trackingActionListener,
            Collections.emptySet()
        );
        RestStatus restStatus = randomFrom(RestStatus.values());
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
        httpResponse.setEntity(
            new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON)
        );
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
        OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
        assertEquals("OpenSearch exception [type=exception, reason=test error message]", openSearchException.getMessage());
        assertEquals(restStatus, openSearchException.status());
        assertSame(responseException, openSearchException.getSuppressed()[0]);
    }

    public void testWrapResponseListenerOnResponseExceptionWithBrokenEntity() throws IOException {
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(),
                trackingActionListener,
                Collections.emptySet()
            );
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            httpResponse.setEntity(new StringEntity("{\"error\":", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            responseListener.onFailure(responseException);
            assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
            OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
            assertEquals("Unable to parse response body", openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getCause());
            assertThat(openSearchException.getSuppressed()[0], instanceOf(JsonParseException.class));
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(),
                trackingActionListener,
                Collections.emptySet()
            );
            RestStatus restStatus = randomFrom(RestStatus.values());
            ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(restStatus.getStatus(), restStatus.name());
            httpResponse.setEntity(new StringEntity("{\"status\":" + restStatus.getStatus() + "}", ContentType.APPLICATION_JSON));
            Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            responseListener.onFailure(responseException);
            assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
            OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
            assertEquals("Unable to parse response body", openSearchException.getMessage());
            assertEquals(restStatus, openSearchException.status());
            assertSame(responseException, openSearchException.getCause());
            assertThat(openSearchException.getSuppressed()[0], instanceOf(IllegalStateException.class));
        }
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnores() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> response.getStatusLine().getStatusCode(),
            trackingActionListener,
            Collections.singleton(404)
        );
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        // although we got an exception, we turn it into a successful response because the status code was provided among ignores
        assertNull(trackingActionListener.exception.get());
        assertEquals(404, trackingActionListener.statusCode.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnoresErrorNoBody() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        // response parsing throws exception while handling ignores. same as when GetResponse#fromXContent throws error when trying
        // to parse a 404 response which contains an error rather than a valid document not found response.
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> { throw new IllegalStateException(); },
            trackingActionListener,
            Collections.singleton(404)
        );
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
        OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
        assertEquals(RestStatus.NOT_FOUND, openSearchException.status());
        assertSame(responseException, openSearchException.getCause());
        assertEquals(responseException.getMessage(), openSearchException.getMessage());
    }

    public void testWrapResponseListenerOnResponseExceptionWithIgnoresErrorValidBody() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        // response parsing throws exception while handling ignores. same as when GetResponse#fromXContent throws error when trying
        // to parse a 404 response which contains an error rather than a valid document not found response.
        ResponseListener responseListener = restHighLevelClient.wrapResponseListener(
            response -> { throw new IllegalStateException(); },
            trackingActionListener,
            Collections.singleton(404)
        );
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(RestStatus.NOT_FOUND.getStatus(), RestStatus.NOT_FOUND.name());
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":404}", ContentType.APPLICATION_JSON));
        Response response = new Response(REQUEST_LINE, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(OpenSearchException.class));
        OpenSearchException openSearchException = (OpenSearchException) trackingActionListener.exception.get();
        assertEquals(RestStatus.NOT_FOUND, openSearchException.status());
        assertSame(responseException, openSearchException.getSuppressed()[0]);
        assertEquals("OpenSearch exception [type=exception, reason=test error message]", openSearchException.getMessage());
    }

    public void testDefaultNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = RestHighLevelClient.getDefaultNamedXContents();
        int expectedInternalAggregations = InternalAggregationTestCase.getDefaultNamedXContents().size();
        int expectedSuggestions = 3;

        assertEquals(expectedInternalAggregations + expectedSuggestions, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals(2, categories.size());
        assertEquals(expectedInternalAggregations, categories.get(Aggregation.class).intValue());
        assertEquals(expectedSuggestions, categories.get(Suggest.Suggestion.class).intValue());
    }

    public void testProvidedNamedXContents() {
        List<NamedXContentRegistry.Entry> namedXContents = RestHighLevelClient.getProvidedNamedXContents();
        assertEquals(13, namedXContents.size());
        Map<Class<?>, Integer> categories = new HashMap<>();
        List<String> names = new ArrayList<>();
        for (NamedXContentRegistry.Entry namedXContent : namedXContents) {
            names.add(namedXContent.name.getPreferredName());
            Integer counter = categories.putIfAbsent(namedXContent.categoryClass, 1);
            if (counter != null) {
                categories.put(namedXContent.categoryClass, counter + 1);
            }
        }
        assertEquals("Had: " + categories, 3, categories.size());
        assertEquals(Integer.valueOf(3), categories.get(Aggregation.class));
        assertTrue(names.contains(ChildrenAggregationBuilder.NAME));
        assertTrue(names.contains(MatrixStatsAggregationBuilder.NAME));
        assertEquals(Integer.valueOf(5), categories.get(EvaluationMetric.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(RecallAtK.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
        assertEquals(Integer.valueOf(5), categories.get(MetricDetail.class));
        assertTrue(names.contains(PrecisionAtK.NAME));
        assertTrue(names.contains(RecallAtK.NAME));
        assertTrue(names.contains(MeanReciprocalRank.NAME));
        assertTrue(names.contains(DiscountedCumulativeGain.NAME));
        assertTrue(names.contains(ExpectedReciprocalRank.NAME));
    }

    public void testApiNamingConventions() throws Exception {
        // this list should be empty once the high-level client is feature complete
        String[] notYetSupportedApi = new String[] {
            "create",
            "get_script_context",
            "get_script_languages",
            "indices.get_upgrade",
            "indices.put_alias",
            "render_search_template",
            "scripts_painless_execute",
            "indices.simulate_template",
            "indices.resolve_index",
            "indices.add_block" };
        // These API are not required for high-level client feature completeness
        String[] notRequiredApi = new String[] {
            "cluster.allocation_explain",
            "cluster.pending_tasks",
            "cluster.reroute",
            "cluster.state",
            "cluster.stats",
            "cluster.post_voting_config_exclusions",
            "cluster.delete_voting_config_exclusions",
            "dangling_indices.delete_dangling_index",
            "dangling_indices.import_dangling_index",
            "dangling_indices.list_dangling_indices",
            "indices.shard_stores",
            "indices.upgrade",
            "indices.recovery",
            "indices.segments",
            "indices.stats",
            "ingest.processor_grok",
            "nodes.info",
            "nodes.stats",
            "nodes.hot_threads",
            "nodes.usage",
            "nodes.reload_secure_settings",
            "search_shards",
            "remote_store.restore",
            "remote_store.stats",
            "cluster.put_weighted_routing",
            "cluster.get_weighted_routing",
            "cluster.delete_weighted_routing",
            "cluster.put_decommission_awareness",
            "cluster.get_decommission_awareness",
            "cluster.delete_decommission_awareness", };
        List<String> booleanReturnMethods = Arrays.asList("security.enable_user", "security.disable_user", "security.change_password");
        Set<String> deprecatedMethods = new HashSet<>();
        deprecatedMethods.add("indices.force_merge");
        deprecatedMethods.add("multi_get");
        deprecatedMethods.add("multi_search");
        deprecatedMethods.add("search_scroll");

        ClientYamlSuiteRestSpec restSpec = ClientYamlSuiteRestSpec.load("/rest-api-spec/api");
        Set<String> apiSpec = restSpec.getApis().stream().map(ClientYamlSuiteRestApi::getName).collect(Collectors.toSet());
        Set<String> apiUnsupported = new HashSet<>(apiSpec);
        Set<String> apiNotFound = new HashSet<>();

        Set<String> topLevelMethodsExclusions = new HashSet<>();
        topLevelMethodsExclusions.add("getLowLevelClient");
        topLevelMethodsExclusions.add("close");

        Map<String, Set<Method>> methods = Arrays.stream(RestHighLevelClient.class.getMethods())
            .filter(
                method -> method.getDeclaringClass().equals(RestHighLevelClient.class)
                    && topLevelMethodsExclusions.contains(method.getName()) == false
            )
            .map(method -> Tuple.tuple(toSnakeCase(method.getName()), method))
            .flatMap(
                tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                    ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType())
                    : Stream.of(tuple)
            )
            .filter(tuple -> tuple.v2().getAnnotation(Deprecated.class) == null)
            .collect(Collectors.groupingBy(Tuple::v1, Collectors.mapping(Tuple::v2, Collectors.toSet())));

        // TODO remove in 8.0 - we will undeprecate indices.get_template because the current getIndexTemplate
        // impl will replace the existing getTemplate method.
        // The above general-purpose code ignores all deprecated methods which in this case leaves `getTemplate`
        // looking like it doesn't have a valid implementatation when it does.
        apiUnsupported.remove("indices.get_template");

        // Synced flush is deprecated
        apiUnsupported.remove("indices.flush_synced");

        for (Map.Entry<String, Set<Method>> entry : methods.entrySet()) {
            String apiName = entry.getKey();

            for (Method method : entry.getValue()) {
                assertTrue(
                    "method [" + apiName + "] is not final",
                    Modifier.isFinal(method.getClass().getModifiers()) || Modifier.isFinal(method.getModifiers())
                );
                assertTrue("method [" + method + "] should be public", Modifier.isPublic(method.getModifiers()));

                // we convert all the method names to snake case, hence we need to look for the '_async' suffix rather than 'Async'
                if (apiName.endsWith("_async")) {
                    assertAsyncMethod(methods, method, apiName);
                } else if (isSubmitTaskMethod(apiName)) {
                    assertSubmitTaskMethod(methods, method, apiName, restSpec);
                } else {
                    assertSyncMethod(method, apiName, booleanReturnMethods);
                    apiUnsupported.remove(apiName);
                    if (apiSpec.contains(apiName) == false) {
                        if (deprecatedMethods.contains(apiName)) {
                            assertTrue(
                                "method [" + method.getName() + "], api [" + apiName + "] should be deprecated",
                                method.isAnnotationPresent(Deprecated.class)
                            );
                        } else {
                            if (// IndicesClientIT.getIndexTemplate should be renamed "getTemplate" in version 8.0 when we
                                // can get rid of 7.0's deprecated "getTemplate"
                            apiName.equals("indices.get_index_template") == false) {
                                apiNotFound.add(apiName);
                            }
                        }
                    }
                }
            }
        }
        assertThat(
            "Some client method doesn't match a corresponding API defined in the REST spec: " + apiNotFound,
            apiNotFound.size(),
            equalTo(0)
        );

        // we decided not to support cat API in the high-level REST client, they are supposed to be used from a low-level client
        apiUnsupported.removeIf(api -> api.startsWith("cat."));
        Stream.concat(Arrays.stream(notYetSupportedApi), Arrays.stream(notRequiredApi))
            .forEach(
                api -> assertTrue(
                    api + " API is either not defined in the spec or already supported by the high-level client",
                    apiUnsupported.remove(api)
                )
            );
        assertThat("Some API are not supported but they should be: " + apiUnsupported, apiUnsupported.size(), equalTo(0));
    }

    private static void assertSyncMethod(Method method, String apiName, List<String> booleanReturnMethods) {
        // A few methods return a boolean rather than a response object
        if (apiName.equals("ping") || apiName.contains("exist") || booleanReturnMethods.contains(apiName)) {
            assertThat(
                "the return type for method [" + method + "] is incorrect",
                method.getReturnType().getSimpleName(),
                equalTo("boolean")
            );
        } else {
            // It's acceptable for 404s to be represented as empty Optionals
            if (!method.getReturnType().isAssignableFrom(Optional.class)) {
                assertThat(
                    "the return type for method [" + method + "] is incorrect",
                    method.getReturnType().getSimpleName(),
                    endsWith("Response")
                );
            }
        }

        assertEquals("incorrect number of exceptions for method [" + method + "]", 1, method.getExceptionTypes().length);
        final Class<?>[] parameterTypes = method.getParameterTypes();
        // a few methods don't accept a request object as argument
        if (APIS_WITHOUT_REQUEST_OBJECT.contains(apiName)) {
            assertEquals("incorrect number of arguments for method [" + method + "]", 1, method.getParameterCount());
            assertThat("the parameter to method [" + method + "] is the wrong type", parameterTypes[0], equalTo(RequestOptions.class));
        } else {
            assertEquals("incorrect number of arguments for method [" + method + "]", 2, method.getParameterCount());
            // This is no longer true for all methods. Some methods can contain these 2 args backwards because of deprecation
            if (parameterTypes[0].equals(RequestOptions.class)) {
                assertThat(
                    "the first parameter to method [" + method + "] is the wrong type",
                    parameterTypes[0],
                    equalTo(RequestOptions.class)
                );
                assertThat(
                    "the second parameter to method [" + method + "] is the wrong type",
                    parameterTypes[1].getSimpleName(),
                    endsWith("Request")
                );
            } else {
                assertThat(
                    "the first parameter to method [" + method + "] is the wrong type",
                    parameterTypes[0].getSimpleName(),
                    endsWith("Request")
                );
                assertThat(
                    "the second parameter to method [" + method + "] is the wrong type",
                    parameterTypes[1],
                    equalTo(RequestOptions.class)
                );
            }
        }
    }

    private static void assertAsyncMethod(Map<String, Set<Method>> methods, Method method, String apiName) {
        assertTrue(
            "async method [" + method.getName() + "] doesn't have corresponding sync method",
            methods.containsKey(apiName.substring(0, apiName.length() - 6))
        );
        assertThat("async method [" + method + "] should return Cancellable", method.getReturnType(), equalTo(Cancellable.class));
        assertEquals("async method [" + method + "] should not throw any exceptions", 0, method.getExceptionTypes().length);
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (APIS_WITHOUT_REQUEST_OBJECT.contains(apiName.replaceAll("_async$", ""))) {
            assertEquals(2, parameterTypes.length);
            assertThat(parameterTypes[0], equalTo(RequestOptions.class));
            assertThat(parameterTypes[1], equalTo(ActionListener.class));
        } else {
            assertEquals("async method [" + method + "] has the wrong number of arguments", 3, method.getParameterCount());
            // This is no longer true for all methods. Some methods can contain these 2 args backwards because of deprecation
            if (parameterTypes[0].equals(RequestOptions.class)) {
                assertThat(
                    "the first parameter to async method [" + method + "] should be a request type",
                    parameterTypes[0],
                    equalTo(RequestOptions.class)
                );
                assertThat(
                    "the second parameter to async method [" + method + "] is the wrong type",
                    parameterTypes[1].getSimpleName(),
                    endsWith("Request")
                );
            } else {
                assertThat(
                    "the first parameter to async method [" + method + "] should be a request type",
                    parameterTypes[0].getSimpleName(),
                    endsWith("Request")
                );
                assertThat(
                    "the second parameter to async method [" + method + "] is the wrong type",
                    parameterTypes[1],
                    equalTo(RequestOptions.class)
                );
            }
            assertThat(
                "the third parameter to async method [" + method + "] is the wrong type",
                parameterTypes[2],
                equalTo(ActionListener.class)
            );
        }
    }

    private static void assertSubmitTaskMethod(
        Map<String, Set<Method>> methods,
        Method method,
        String apiName,
        ClientYamlSuiteRestSpec restSpec
    ) {
        String methodName = extractMethodName(apiName);
        final Class<?>[] parameterTypes = method.getParameterTypes();
        assertTrue("submit task method [" + method.getName() + "] doesn't have corresponding sync method", methods.containsKey(methodName));
        assertEquals("submit task method [" + method + "] has the wrong number of arguments", 2, method.getParameterCount());
        assertThat(
            "the first parameter to submit task method [" + method + "] is the wrong type",
            parameterTypes[0].getSimpleName(),
            endsWith("Request")
        );
        assertThat(
            "the second parameter to submit task method [" + method + "] is the wrong type",
            parameterTypes[1],
            equalTo(RequestOptions.class)
        );

        assertThat(
            "submit task method [" + method + "] must have wait_for_completion parameter in rest spec",
            restSpec.getApi(methodName).getParams(),
            Matchers.hasKey("wait_for_completion")
        );
    }

    private static String extractMethodName(String apiName) {
        return apiName.substring(SUBMIT_TASK_PREFIX.length(), apiName.length() - SUBMIT_TASK_SUFFIX.length());
    }

    private static boolean isSubmitTaskMethod(String apiName) {
        return apiName.startsWith(SUBMIT_TASK_PREFIX) && apiName.endsWith(SUBMIT_TASK_SUFFIX);
    }

    private static Stream<Tuple<String, Method>> getSubClientMethods(String namespace, Class<?> clientClass) {
        return Arrays.stream(clientClass.getMethods())
            .filter(method -> method.getDeclaringClass().equals(clientClass))
            .map(method -> Tuple.tuple(namespace + "." + toSnakeCase(method.getName()), method))
            .flatMap(
                tuple -> tuple.v2().getReturnType().getName().endsWith("Client")
                    ? getSubClientMethods(tuple.v1(), tuple.v2().getReturnType())
                    : Stream.of(tuple)
            );
    }

    private static String toSnakeCase(String camelCase) {
        StringBuilder snakeCaseString = new StringBuilder();
        for (Character aChar : camelCase.toCharArray()) {
            if (Character.isUpperCase(aChar)) {
                snakeCaseString.append('_');
                snakeCaseString.append(Character.toLowerCase(aChar));
            } else {
                snakeCaseString.append(aChar);
            }
        }
        return snakeCaseString.toString();
    }

    private static class TrackingActionListener implements ActionListener<Integer> {
        private final AtomicInteger statusCode = new AtomicInteger(-1);
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onResponse(Integer statusCode) {
            assertTrue(this.statusCode.compareAndSet(-1, statusCode));
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue(exception.compareAndSet(null, e));
        }
    }

    private static StatusLine newStatusLine(RestStatus restStatus) {
        return new StatusLine(HTTP_PROTOCOL, restStatus.getStatus(), restStatus.name());
    }
}
