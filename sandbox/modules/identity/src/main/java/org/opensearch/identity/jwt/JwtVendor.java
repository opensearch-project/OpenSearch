/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.jwt;

import org.apache.cxf.jaxrs.json.basic.JsonMapObjectReaderWriter;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;

import java.time.Instant;
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JwtVendor {
    private static final Logger logger = LogManager.getLogger(JwtVendor.class);

    private static JsonMapObjectReaderWriter jsonMapReaderWriter = new JsonMapObjectReaderWriter();

    static JsonWebKey getDefaultJsonWebKeyWithSigningKey(String signingKey) {
        JsonWebKey jwk = new JsonWebKey();

        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("HS512");
        jwk.setPublicKeyUse(PublicKeyUse.SIGN);
        jwk.setProperty("k", signingKey);
        return jwk;
    }

    public static String createJwt(Map<String, String> claims, String signingKey) {
        JoseJwtProducer jwtProducer = new JoseJwtProducer();
        jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(getDefaultJsonWebKeyWithSigningKey(signingKey)));
        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(System.currentTimeMillis() / 1000);
        long expiryTime = System.currentTimeMillis() / 1000 + (60 * 60);
        jwtClaims.setExpiryTime(expiryTime);

        if (claims.containsKey("sub")) {
            jwtClaims.setProperty("sub", claims.get("sub"));
        } else {
            // TODO What exception should be thrown and how should it be handled?
            jwtClaims.setProperty("sub", "example_subject");
        }

        if (claims.containsKey("iat")) {
            jwtClaims.setProperty("iat", claims.get("iat"));
        } else {
            jwtClaims.setProperty("iat", Instant.now().toString());
        }

        String encodedJwt = jwtProducer.processJwt(jwt);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Created JWT: "
                    + encodedJwt
                    + "\n"
                    + jsonMapReaderWriter.toJson(jwt.getJwsHeaders())
                    + "\n"
                    + JwtUtils.claimsToJson(jwt.getClaims())
            );
        }
        return encodedJwt;
    }
}
