/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.identity.tokens.AuthToken;

import java.io.IOException;

/**
 * Noop implementation of an Auth Token
 */
public class NoopAuthToken extends AuthToken {
    public static final String NAME = "noop_auth_token";

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
