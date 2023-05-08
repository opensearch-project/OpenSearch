/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

/**
 * A NoopToken is a pass-through AuthToken
 */
public class NoopToken implements AuthToken {
    public final static String TOKEN_IDENTIFIER = "Noop";

    /**
     * Returns the TokenIdentifier of Noop
     * @return The token identifier "Noop"
     */
    public String getTokenIdentifier() {
        return TOKEN_IDENTIFIER;
    }

}
