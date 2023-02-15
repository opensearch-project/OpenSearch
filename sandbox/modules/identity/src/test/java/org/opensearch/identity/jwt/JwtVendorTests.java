/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.jwt;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.opensearch.identity.JwtVendorTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class JwtVendorTests extends OpenSearchTestCase {

    public void testCreateJwtWithClaims() {
        Map<String, String> jwtClaims = new HashMap<>();
        jwtClaims.put("sub", "testSubject");

        String encodedToken = JwtVendor.createJwt(jwtClaims, JwtVendorTestUtils.SIGNING_KEY);

        JwtVerifier verifier = new JwtVerifier(JwtVendorTestUtils.SIGNING_KEY);

        try {
            JwtToken token = verifier.getVerifiedJwtToken(encodedToken);
            assertTrue(token.getClaims().getClaim("sub").equals("testSubject"));
        } catch (BadCredentialsException e) {
            fail("Unexpected BadCredentialsException thrown");
        }
    }
}
