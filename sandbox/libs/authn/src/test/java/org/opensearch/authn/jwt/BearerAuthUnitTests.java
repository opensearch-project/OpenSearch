/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.jwt;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationToken;
import org.opensearch.authn.HttpHeaderToken;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.authn.AuthenticationTokenHandler.extractShiroAuthToken;

public class BearerAuthUnitTests extends OpenSearchTestCase {

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
}
