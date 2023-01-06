/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.jwt;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.cxf.rs.security.jose.jwa.SignatureAlgorithm;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.KeyType;
import org.apache.cxf.rs.security.jose.jwk.PublicKeyUse;
import org.apache.cxf.rs.security.jose.jws.JwsJwtCompactConsumer;
import org.apache.cxf.rs.security.jose.jws.JwsSignatureVerifier;
import org.apache.cxf.rs.security.jose.jws.JwsUtils;
import org.apache.cxf.rs.security.jose.jwt.JwtClaims;
import org.apache.cxf.rs.security.jose.jwt.JwtException;
import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.cxf.rs.security.jose.jwt.JwtUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JwtVerifier {
    private final static Logger log = LogManager.getLogger(JwtVerifier.class);

    public static JwtToken getVerifiedJwtToken(String encodedJwt) throws BadCredentialsException {
        try {
            JwsJwtCompactConsumer jwtConsumer = new JwsJwtCompactConsumer(encodedJwt);
            JwtToken jwt = jwtConsumer.getJwtToken();

            String escapedKid = jwt.getJwsHeaders().getKeyId();
            String kid = escapedKid;
            if (!(kid == null || kid.isBlank())) {
                kid = StringEscapeUtils.unescapeJava(escapedKid);
            }
            JsonWebKey key = JwtVendor.getDefaultJsonWebKey();

            // Algorithm is not mandatory for the key material, so we set it to the same as the JWT
            if (key.getAlgorithm() == null && key.getPublicKeyUse() == PublicKeyUse.SIGN && key.getKeyType() == KeyType.RSA) {
                key.setAlgorithm(jwt.getJwsHeaders().getAlgorithm());
            }

            JwsSignatureVerifier signatureVerifier = getInitializedSignatureVerifier(key, jwt);

            boolean signatureValid = jwtConsumer.verifySignatureWith(signatureVerifier);

            if (!signatureValid) {
                throw new BadCredentialsException("Invalid JWT signature");
            }

            validateClaims(jwt);

            return jwt;
        } catch (JwtException e) {
            throw new BadCredentialsException(e.getMessage(), e);
        }
    }

    private static void validateSignatureAlgorithm(JsonWebKey key, JwtToken jwt) throws BadCredentialsException {
        if (key.getAlgorithm() == null || key.getAlgorithm().isBlank()) {
            return;
        }

        SignatureAlgorithm keyAlgorithm = SignatureAlgorithm.getAlgorithm(key.getAlgorithm());
        SignatureAlgorithm tokenAlgorithm = SignatureAlgorithm.getAlgorithm(jwt.getJwsHeaders().getAlgorithm());

        if (!keyAlgorithm.equals(tokenAlgorithm)) {
            throw new BadCredentialsException(
                "Algorithm of JWT does not match algorithm of JWK (" + keyAlgorithm + " != " + tokenAlgorithm + ")"
            );
        }
    }

    private static JwsSignatureVerifier getInitializedSignatureVerifier(JsonWebKey key, JwtToken jwt) throws BadCredentialsException,
        JwtException {

        validateSignatureAlgorithm(key, jwt);
        JwsSignatureVerifier result = JwsUtils.getSignatureVerifier(key, jwt.getJwsHeaders().getSignatureAlgorithm());
        if (result == null) {
            throw new BadCredentialsException("Cannot verify JWT");
        } else {
            return result;
        }
    }

    private static void validateClaims(JwtToken jwt) {
        JwtClaims claims = jwt.getClaims();

        if (claims != null) {
            JwtUtils.validateJwtExpiry(claims, 0, false);
            JwtUtils.validateJwtNotBefore(claims, 0, false);
        }
    }
}
