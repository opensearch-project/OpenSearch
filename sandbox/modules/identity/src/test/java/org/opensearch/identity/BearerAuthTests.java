/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationToken;
import org.hamcrest.MatcherAssert;
import org.opensearch.authn.jwt.JwtVendor;
import org.opensearch.authn.jwt.JwtVerifier;
import org.opensearch.authn.tokens.BearerAuthToken;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.authn.AuthenticationTokenHandler.extractShiroAuthToken;

public class BearerAuthTests extends AbstractIdentityTestCase {

    public void testExpiredValidJwt() {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createExpiredJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;
        BearerAuthToken bearerAuthToken = new BearerAuthToken(headerBody);

        try {
            AuthenticationToken extractedShiroToken = extractShiroAuthToken(bearerAuthToken);
        } catch (RuntimeException ex) {
            assertFalse(ex.getMessage().isEmpty());
            assertEquals("The token has expired", ex.getMessage());
        }
    }

    public void testEarlyValidJwt() {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createEarlyJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;
        BearerAuthToken bearerAuthToken = new BearerAuthToken(headerBody);

        try {
            AuthenticationToken extractedShiroToken = extractShiroAuthToken(bearerAuthToken);
        } catch (RuntimeException ex) {
            assertFalse(ex.getMessage().isEmpty());
            assertEquals("The token cannot be accepted yet", ex.getMessage());
        }
    }

    public void testValidJwt() {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;
        BearerAuthToken bearerAuthToken = new BearerAuthToken(headerBody);
        AuthenticationToken extractedShiroToken;

        try {
            extractedShiroToken = extractShiroAuthToken(bearerAuthToken); // This should verify and then extract the shiro token for login
            assertEquals(encodedToken, extractedShiroToken.getPrincipal());
        } catch (RuntimeException ex) {
            throw new Error(ex);
        }
        if (extractedShiroToken == null) {
            throw new Error("The value of the extracted token is null.");
        }
    }

    public void testInvalidJwt() {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createInvalidJwt(jwtClaims);
        try {
            JwtToken token = JwtVerifier.getVerifiedJwtToken(encodedToken);
        } catch (RuntimeException ex) {
            assertFalse(ex.getMessage().isEmpty());
            assertEquals("Algorithm of JWT does not match algorithm of JWK (HS512 != HS256)", ex.getMessage());
        }
    }

    public void testClusterHealthWithValidBearerAuthenticationHeader() throws IOException {
        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);

        Response response = getRestClient().performRequest(request);

        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        assertThat(content, containsString("green"));

    }

    public void testClusterHealthWithExpiredBearerAuthenticationHeader() throws IOException {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createExpiredJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);
        // Should be unauthorized since JWT is expired
        try {
            getRestClient().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        }
    }

    public void testClusterHealthWithInvalidBearerAuthenticationHeader() throws IOException {

        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createInvalidJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);
        try {
            getRestClient().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        }
    }

    public void testClusterHealthWithCorruptBearerAuthenticationHeader() throws IOException {

        String headerBody = "Bearer NotAJWT";

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);
        try {
            getRestClient().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        }
    }
}
