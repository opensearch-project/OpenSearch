/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * This class represents the claims of an OnBehalfOf token.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OnBehalfOfClaims {

    private final String audience;
    private final Long expiration_seconds;

    /**
     * Constructor for OnBehalfOfClaims
     * @param aud the Audience for the token
     * @param expiration_seconds the length of time in seconds the token is valid

     */
    public OnBehalfOfClaims(String aud, Long expiration_seconds) {
        this.audience = aud;
        this.expiration_seconds = expiration_seconds;
    }

    /**
     * A constructor which sets the default expiration time of 5 minutes from the current time
     * @param aud the Audience for the token
     * @param subject the subject of the token
     */
    public OnBehalfOfClaims(String aud, String subject) {
        this(aud, 300L);
    }

    public String getAudience() {
        return audience;
    }

    public Long getExpiration() {
        return expiration_seconds;
    }
}
