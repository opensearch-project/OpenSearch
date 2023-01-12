/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.cxf.jaxrs.json.basic.JsonMapObjectReaderWriter;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JoseJwtProducer;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.identity.jwt.JwtVendor;

public class JwtVendorTestUtils {
    private static final Logger logger = LogManager.getLogger(JwtVendor.class);

    private static JsonMapObjectReaderWriter jsonMapReaderWriter = new JsonMapObjectReaderWriter();

    static JsonWebKey getDefaultJsonWebKey() {
        JsonWebKey jwk = new JsonWebKey();

        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("HS512");
        jwk.setPublicKeyUse(PublicKeyUse.SIGN);
        String b64SigningKey = Base64.getEncoder().encodeToString("exchangeKey".getBytes(StandardCharsets.UTF_8));
        jwk.setProperty("k", b64SigningKey);
        return jwk;
    }

    static JsonWebKey getHS256WebKey() {
        JsonWebKey jwk = new JsonWebKey();
        jwk.setKeyType(KeyType.OCTET);
        jwk.setAlgorithm("HS256");
        jwk.setPublicKeyUse(PublicKeyUse.SIGN);
        String b64SigningKey = Base64.getEncoder().encodeToString("exchangeKey".getBytes(StandardCharsets.UTF_8));
        jwk.setProperty("k", b64SigningKey);
        return jwk;
    }

    /**
     * Creates a Jwt that will not be valid until a year after it is created--this means it should not be usable for authentication
     */
    public static String createEarlyJwt(Map<String, String> claims) {
        JoseJwtProducer jwtProducer = new JoseJwtProducer();
        jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(getDefaultJsonWebKey()));
        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(System.currentTimeMillis() / 1000 + 60 * 60 * 24 * 365); // Not valid until a year from creation
        long expiryTime = System.currentTimeMillis() / 1000 + (60 * 60);
        jwtClaims.setExpiryTime(expiryTime);

        if (claims.containsKey("sub")) {
            jwtClaims.setProperty("sub", claims.get("sub"));
        } else {
            jwtClaims.setProperty("sub", "example_subject");
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

    /**
     * Creates a Jwt that has already expired upon creation--this means it should not be usable for authentication
     */
    public static String createExpiredJwt(Map<String, String> claims) {
        JoseJwtProducer jwtProducer = new JoseJwtProducer();
        jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(getDefaultJsonWebKey()));
        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        long expiryTime = System.currentTimeMillis() / 1000 - 1; // This means the token expired a second before it was made so should never
        // be valid
        jwtClaims.setExpiryTime(expiryTime);

        if (claims.containsKey("sub")) {
            jwtClaims.setProperty("sub", claims.get("sub"));
        } else {
            jwtClaims.setProperty("sub", "example_subject");
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

    /**
     * Creates a Jwt which uses HS256 as its signature algorithm--this is not expected by the verifier so should result in a key mismatch
     */
    public static String createInvalidJwt(Map<String, String> claims) {
        JoseJwtProducer jwtProducer = new JoseJwtProducer();
        jwtProducer.setSignatureProvider(JwsUtils.getSignatureProvider(getHS256WebKey()));
        JwtClaims jwtClaims = new JwtClaims();
        JwtToken jwt = new JwtToken(jwtClaims);

        jwtClaims.setNotBefore(System.currentTimeMillis() / 1000);
        long expiryTime = System.currentTimeMillis() / 1000 + (60 * 60);
        jwtClaims.setExpiryTime(expiryTime);

        if (claims.containsKey("sub")) {
            jwtClaims.setProperty("sub", claims.get("sub"));
        } else {
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
