/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.type;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationToken;
import org.hamcrest.MatcherAssert;
import org.opensearch.authn.HttpHeaderToken;
import org.opensearch.authn.jwt.BadCredentialsException;
import org.opensearch.authn.jwt.JwtVendor;
import org.opensearch.authn.jwt.JwtVerifier;
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

public class BearerAuthenticationTests extends OpenSearchTestCase {

    public void testExpiredValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createExpiredJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;
        HttpHeaderToken HttpToken = new HttpHeaderToken(headerBody); // Create an HttpHeaderToken that just holds the 'Bearer' + JWT

        try {
            AuthenticationToken extractedShiroToken = extractShiroAuthToken(HttpToken); // This should verify and then extract the shiro token for login -- should fail because expired
        } catch (BadCredentialsException ex) {

            assertFalse(ex.getMessage().isEmpty());
            assertEquals("The token has expired", ex.getMessage());
        }
    }

    public void testEarlyValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createEarlyJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;
        HttpHeaderToken HttpToken = new HttpHeaderToken(headerBody); // Create an HttpHeaderToken that just holds the 'Bearer' + JWT

        try {
            AuthenticationToken extractedShiroToken = extractShiroAuthToken(HttpToken); // This should verify and then extract the shiro token for login -- should fail because expired
        } catch (BadCredentialsException ex) {

            assertFalse(ex.getMessage().isEmpty());
            assertEquals("The token cannot be accepted yet", ex.getMessage());
        }
    }

    public AuthenticationToken testValidJwt() {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;
        HttpHeaderToken HttpToken = new HttpHeaderToken(headerBody); // Create an HttpHeaderToken that just holds the 'Bearer' + JWT
        AuthenticationToken extractedShiroToken = null;
        try {
            extractedShiroToken = extractShiroAuthToken(HttpToken); // This should verify and then extract the shiro token for login
        } catch (BadCredentialsException ex) {
            throw new Error(ex);
        }
        if (extractedShiroToken == null) {
            throw new Error("The value of the extracted token is null after try/catch.");
        }
        return extractedShiroToken; // Should not be null
    }

    public void testInvalidJwt() {
        //TODO: Token should fail because of attempt to verify incorrect signature
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");
        jwtClaims.put("key", "HSA512");


        String encodedToken = JwtVendor.createJwt(jwtClaims);

        try {
            JwtToken token = JwtVerifier.getVerifiedJwtToken(encodedToken);
        } catch (BadCredentialsException e) {
            fail("Unexpected BadCredentialsException thrown");
        }
    }

    public void testClusterHealthWithValidBearerAuthenticationHeader() throws IOException {
        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build(); // This needs to be a built JWT
        request.setOptions(options);
        Response response = OpenSearchRestTestCase.client().performRequest(request);

        OpenSearchRestTestCase.assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithExpiredBearerAuthenticationHeader() throws IOException {

        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createExpiredJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build(); //Not sure what this will do but it definitely should not pass

        request.setOptions(options);
        Response response = OpenSearchRestTestCase.client().performRequest(request);

        // Should not fail, because current implementation allows a unauthorized request to pass
        // TODO: Update this to test for UNAUTHORIZED once that flow is implemented
        OpenSearchRestTestCase.assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithInvalidBearerAuthenticationHeader() throws IOException { // Should have this use the createInvalidJwt method once that is created.

        Map<String, Object> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createExpiredJwt(jwtClaims);

        String headerBody = "Bearer " + encodedToken;

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build(); //Not sure what this will do but it definitely should not pass

        request.setOptions(options);
        Response response = OpenSearchRestTestCase.client().performRequest(request);

        // Should not fail, because current implementation allows a unauthorized request to pass
        // TODO: Update this to test for UNAUTHORIZED once that flow is implemented
        OpenSearchRestTestCase.assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(OpenSearchRestTestCase.entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithCorruptBearerAuthenticationHeader() throws IOException {

        String headerBody = "Bearer NotAJWT";

        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerBody).build(); //Not sure what this will do but it definitely should not pass

        request.setOptions(options);
        try {
            OpenSearchRestTestCase.client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(405));
        }
    }
}
