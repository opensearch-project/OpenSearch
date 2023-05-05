/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

public class NoopToken implements AuthToken {
    public final static String TOKEN_IDENTIFIER = "Noop";

    public String getTokenIdentifier() {
        return TOKEN_IDENTIFIER;
    }

}
