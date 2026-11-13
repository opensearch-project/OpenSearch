/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.termsenum.TermsEnumRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RestTermsEnumActionTests extends OpenSearchTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(org.opensearch.common.settings.Settings.EMPTY, List.of()).getNamedXContents());
    }

    private FakeRestRequest request(Map<String, String> params, String json) {
        FakeRestRequest.Builder builder = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/a,b/_terms_enum")
            .withParams(params);
        if (json != null) {
            builder.withContent(new BytesArray(json), MediaTypeRegistry.JSON);
        }
        return builder.build();
    }

    public void testRoutes() {
        Set<String> routes = new RestTermsEnumAction().routes()
            .stream()
            .map(r -> r.getMethod() + " " + r.getPath())
            .collect(Collectors.toSet());
        assertEquals(Set.of("GET /{index}/_terms_enum", "POST /{index}/_terms_enum"), routes);
        assertEquals("terms_enum_action", new RestTermsEnumAction().getName());
    }

    public void testParsesBody() throws IOException {
        TermsEnumRequest r = RestTermsEnumAction.parseRequest(
            request(
                Map.of("index", "a,b", "ignore_unavailable", "true"),
                "{\"field\":\"host\",\"string\":\"web-\",\"size\":25,\"case_insensitive\":true,\"timeout\":\"250ms\","
                    + "\"search_after\":\"web-03\",\"index_filter\":{\"term\":{\"env\":\"prod\"}}}"
            )
        );
        assertArrayEquals(new String[] { "a", "b" }, r.indices());
        assertEquals("host", r.field());
        assertEquals("web-", r.string());
        assertEquals(25, r.size());
        assertTrue(r.caseInsensitive());
        assertEquals(TimeValue.timeValueMillis(250), r.timeout());
        assertEquals("web-03", r.searchAfter());
        assertThat(r.indexFilter(), Matchers.instanceOf(TermQueryBuilder.class));
        assertTrue(r.indicesOptions().ignoreUnavailable());
        assertTrue(r.indicesOptions().forbidClosedIndices());
    }

    public void testDefaultsWithMinimalBody() throws IOException {
        TermsEnumRequest r = RestTermsEnumAction.parseRequest(request(Map.of("index", "a"), "{\"field\":\"host\"}"));
        assertEquals("host", r.field());
        assertEquals("", r.string());
        assertEquals(TermsEnumRequest.DEFAULT_SIZE, r.size());
        assertFalse(r.caseInsensitive());
        assertEquals(TermsEnumRequest.DEFAULT_TIMEOUT, r.timeout());
        assertNull(r.indexFilter());
        assertNull(r.searchAfter());
        assertEquals(IndicesOptions.strictExpandOpenAndForbidClosed(), r.indicesOptions());
        assertNull(r.validate());
    }

    public void testUnknownBodyFieldIsRejected() {
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> RestTermsEnumAction.parseRequest(request(Map.of("index", "a"), "{\"field\":\"f\",\"bogus\":1}"))
        );
        assertThat(e.getMessage(), Matchers.containsString("bogus"));
    }

    public void testMissingBodyFailsValidation() throws IOException {
        TermsEnumRequest r = RestTermsEnumAction.parseRequest(request(Map.of("index", "a"), null));
        assertThat(r.validate().getMessage(), Matchers.containsString("[field] is required"));
    }

    public void testRangeIndexFilterIsParsed() throws IOException {
        TermsEnumRequest r = RestTermsEnumAction.parseRequest(
            request(Map.of("index", "a"), "{\"field\":\"f\",\"index_filter\":{\"range\":{\"@timestamp\":{\"gte\":\"now-1d\"}}}}")
        );
        assertThat(r.indexFilter(), Matchers.instanceOf(RangeQueryBuilder.class));
    }
}
