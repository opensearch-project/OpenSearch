/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.tokens;

import org.opensearch.common.io.stream.NamedWriteable;

/**
 * Interface for all token formats to support to authenticate user such as UserName/Password tokens, Access tokens, and more.
 *
 * @opensearch.experimental
 */
public abstract class AuthToken implements NamedWriteable {

    public TokenType getTokenType() {
        return TokenType.NONE;
    }

    public String getTokenValue() {
        return "";
    }

}
