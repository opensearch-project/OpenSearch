/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

/**
 * This enum represents the standard claims that are used in the JWTs for OnBehalfOf tokens
 */
public enum StandardTokenClaims {

    ISSUER("iss"),
    SUBJECT("sub"),
    AUDIENCE("aud"),
    EXPIRATION_TIME("exp"),
    NOT_BEFORE("nbf"),
    ISSUED_AT("iat"),
    JWT_ID("jti");

    private final String name;

    StandardTokenClaims(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name;
    }

    public static StandardTokenClaims fromString(String name) {
        return StandardTokenClaims.valueOf(name);
    }
}
