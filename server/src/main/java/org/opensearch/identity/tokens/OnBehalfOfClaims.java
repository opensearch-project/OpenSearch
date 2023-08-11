/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

/**
 * This class represents the claims of an OnBehalfOf token.
 */
public class OnBehalfOfClaims {

    private final String audience;
    private final String subject;
    private final Long expiration;
    private final Long not_before;
    private final Long issued_at;

    /**
     * Constructor for OnBehalfOfClaims
     * @param aud the Audience for the token
     * @param subject the subject of the token
     * @param expiration the expiration time in seconds for the token
     * @param not_before the not_before time in seconds for the token
     * @param issued_at the issued_at time in seconds for the token
     */
    public OnBehalfOfClaims(String aud, String subject, Long expiration, Long not_before, Long issued_at) {
        this.audience = aud;
        this.subject = subject;
        this.expiration = expiration;
        this.not_before = not_before;
        this.issued_at = issued_at;
    }

    /**
     * A constructor that sets a default issued at time of the current time
     * @param aud the Audience for the token
     * @param subject the subject of the token
     * @param expiration the expiration time in seconds for the token
     * @param not_before the not_before time in seconds for the token
     */
    public OnBehalfOfClaims(String aud, String subject, Long expiration, Long not_before) {
        this(aud, subject, expiration, not_before, System.currentTimeMillis() / 1000);
    }

    /**
     * A constructor which sets a default not before time of the current time
     * @param aud the Audience for the token
     * @param subject the subject of the token
     * @param expiration the expiration time in seconds for the token
     */
    public OnBehalfOfClaims(String aud, String subject, Long expiration) {
        this(aud, subject, expiration, System.currentTimeMillis() / 1000);
    }

    /**
     * A constructor which sets the default expiration time of 5 minutes from the current time
     * @param aud the Audience for the token
     * @param subject the subject of the token
     */
    public OnBehalfOfClaims(String aud, String subject) {
        this(aud, subject, System.currentTimeMillis() / 1000 + 300);
    }

    public String getAudience() {
        return audience;
    }

    public String getSubject() {
        return subject;
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
