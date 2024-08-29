/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestTokenExtractorTests extends OpenSearchTestCase {

    public void testAuthorizationHeaderExtractionWithBasicAuthToken() {
        String basicAuthHeader = Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(RestTokenExtractor.AUTH_HEADER_NAME, List.of(BasicAuthToken.TOKEN_IDENTIFIER + " " + basicAuthHeader))
        ).build();
        AuthToken extractedToken = RestTokenExtractor.extractToken(fakeRequest);
        assertThat(extractedToken, instanceOf(BasicAuthToken.class));
        assertThat(extractedToken.asAuthHeaderValue(), equalTo(basicAuthHeader));
    }

    public void testAuthorizationHeaderExtractionWithUnknownToken() {
        String authHeader = "foo";
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(RestTokenExtractor.AUTH_HEADER_NAME, List.of(authHeader))
        ).build();
        AuthToken extractedToken = RestTokenExtractor.extractToken(fakeRequest);
        assertNull(extractedToken);
    }
}
