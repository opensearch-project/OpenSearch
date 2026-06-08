/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for invalid stat name rejection.
 *
 * <p>Feature: datafusion-cluster-stats, Property 4: Invalid stat name rejection
 *
 * <p>For any string that is not one of the 9 valid stat section names, when included
 * in the {@code stat} path parameter, the REST handler SHALL return an HTTP 400 response
 * whose body lists all valid stat names.
 *
 * <p><b>Validates: Requirements 3.3</b>
 */
public class InvalidStatNameRejectionPropertyTests {

    /** All 9 valid stat section names. */
    private static final Set<String> VALID_STAT_NAMES = Set.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "datanode_gate",
        "coordinator_gate",
        "disk_spill"
    );

    // ---- Generators ----

    /**
     * Generates arbitrary non-empty strings that are NOT in the valid stat names set.
     * Includes alphanumeric strings, strings with special characters, and strings
     * that are close to valid names but not exact matches.
     */
    @Provide
    Arbitrary<String> invalidStatNames() {
        return Arbitraries.oneOf(
            // Random alphanumeric strings
            Arbitraries.strings().alpha().numeric().ofMinLength(1).ofMaxLength(50),
            // Strings with underscores (similar to valid names but different)
            Arbitraries.strings().withChars("abcdefghijklmnopqrstuvwxyz_0123456789").ofMinLength(1).ofMaxLength(30),
            // Strings with special characters
            Arbitraries.strings().ascii().ofMinLength(1).ofMaxLength(30)
        ).filter(s -> !s.isEmpty() && !VALID_STAT_NAMES.contains(s));
    }

    // ---- Property 4: Invalid stat name rejection ----

    /**
     * Feature: datafusion-cluster-stats, Property 4: Invalid stat name rejection
     *
     * <p>For any string not in the 9 valid stat section names, the REST handler
     * returns HTTP 400 listing valid names.
     *
     * <p><b>Validates: Requirements 3.3</b>
     */
    @Property(tries = 150)
    void invalidStatNameReturnsHttp400WithValidNamesList(@ForAll("invalidStatNames") String invalidStat) throws Exception {
        RestDataFusionStatsAction handler = new RestDataFusionStatsAction();

        // Build a RestRequest with the invalid stat as the "stat" path parameter
        Map<String, String> params = new HashMap<>();
        params.put("stat", invalidStat);

        RestRequest request = buildFakeRestRequest(params);

        // Call handleRequest (public, inherited from BaseRestHandler) with null NodeClient.
        // Validation happens before client.execute() is called, so null is safe here.
        CapturingRestChannel channel = new CapturingRestChannel(request);
        handler.handleRequest(request, channel, null);

        // Verify HTTP 400 status
        assertNotNull(channel.getResponse(), "Channel must have received a response");
        assertEquals(RestStatus.BAD_REQUEST, channel.getResponse().status(), "Invalid stat '" + invalidStat + "' must produce HTTP 400");

        // Verify the response body lists valid stat names
        String responseBody = channel.getResponse().content().utf8ToString();
        assertTrue(
            responseBody.contains("Invalid stat sections"),
            "Response must contain 'Invalid stat sections' message. Got: " + responseBody
        );
        assertTrue(
            responseBody.contains(RestDataFusionStatsAction.VALID_STATS),
            "Response must list all valid stat names. Got: " + responseBody
        );
    }

    // ---- Test infrastructure ----

    /**
     * Builds a minimal {@link RestRequest} with the given parameters.
     * This avoids depending on the test framework's FakeRestRequest.
     */
    private static RestRequest buildFakeRestRequest(Map<String, String> params) {
        HttpRequest httpRequest = new MinimalHttpRequest();
        HttpChannel httpChannel = new MinimalHttpChannel();
        return new RestRequest(NamedXContentRegistry.EMPTY, params, httpRequest.uri(), httpRequest.getHeaders(), httpRequest, httpChannel) {
        };
    }

    /**
     * A minimal {@link AbstractRestChannel} that captures the response.
     */
    private static class CapturingRestChannel extends AbstractRestChannel {
        private RestResponse response;

        CapturingRestChannel(RestRequest request) {
            super(request, true);
        }

        @Override
        public void sendResponse(RestResponse response) {
            this.response = response;
        }

        public RestResponse getResponse() {
            return response;
        }
    }

    /**
     * Minimal {@link HttpRequest} implementation for property tests.
     */
    private static class MinimalHttpRequest implements HttpRequest {
        @Override
        public RestRequest.Method method() {
            return RestRequest.Method.GET;
        }

        @Override
        public String uri() {
            return "/_plugins/_analytics_backend_datafusion/stats/test";
        }

        @Override
        public BytesReference content() {
            return BytesArray.EMPTY;
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return Collections.emptyMap();
        }

        @Override
        public List<String> strictCookies() {
            return Collections.emptyList();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            return this;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return new HttpResponse() {
                @Override
                public void addHeader(String name, String value) {}

                @Override
                public boolean containsHeader(String name) {
                    return false;
                }
            };
        }

        @Override
        public void release() {}

        @Override
        public HttpRequest releaseAndCopy() {
            return this;
        }

        @Override
        public Exception getInboundException() {
            return null;
        }
    }

    /**
     * Minimal {@link HttpChannel} implementation for property tests.
     */
    private static class MinimalHttpChannel implements HttpChannel {
        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {}

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {}
    }
}
