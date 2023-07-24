/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

public class OnBehalfOfClaims {

    private final String audience;
    private final String issuer;
    private final Long expiration;
    private final Long not_before;
    private final Long issued_at;

    public OnBehalfOfClaims(String aud, String issuer, Long expiration, Long not_before, Long issued_at) {
        this.audience = aud;
        this.issuer = issuer;
        this.expiration = expiration;
        this.not_before = not_before;
        this.issued_at = issued_at;
    }

    public OnBehalfOfClaims(String aud, String issuer, Long expiration, Long not_before) {
        this(aud, issuer, expiration, not_before, System.nanoTime() / 1000000);
    }

    public OnBehalfOfClaims(String aud, String issuer, Long expiration) {
        this(aud, issuer, expiration, System.nanoTime() / 1000000);
    }

    public OnBehalfOfClaims(String aud, String issuer) {
        this(aud, issuer, System.nanoTime() / 1000000 + 300000);
    }

    public String getAudience() {
        return audience;
    }

    public String getIssuer() {
        return issuer;
    }

    public Long getExpiration() {
        return expiration;
    }

    public Long getNot_before() {
        return not_before;
    }

    public Long getIssued_at() {
        return issued_at;
    }
}
