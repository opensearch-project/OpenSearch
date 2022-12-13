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
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.authn.AuthenticationTokenHandler.extractShiroAuthToken;

public class BearerAuthTests extends OpenSearchTestCase {

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
            assertEquals("Invalid JWT signature", ex.getMessage());
        }
        throw new Error("Function should have failed when trying to verify JWT with incorrect signature.");
    }

    public void testClusterHealthWithValidBearerAuthenticationHeader() throws IOException {
        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        String encodedToken = JwtVendor.createJwt(jwtClaims);
        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);
        Response response = OpenSearchRestTestCase.client().performRequest(request);
        OpenSearchRestTestCase.assertOK(response);
        // Standard cluster health response
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).get("status"), equalTo("green"));

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
            OpenSearchRestTestCase.client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
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
        Response response = OpenSearchRestTestCase.client().performRequest(request);
        // Should be unauthorized because created with a different signing algorithm
        //  Current implementation allows a unauthorized request to pass
        try {
            OpenSearchRestTestCase.client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        }
    }

    public void testClusterHealthWithCorruptBearerAuthenticationHeader() throws IOException {

        String headerBody = "Bearer NotAJWT";

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build();
        request.setOptions(options);
        try {
            OpenSearchRestTestCase.client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        }
    }
}
