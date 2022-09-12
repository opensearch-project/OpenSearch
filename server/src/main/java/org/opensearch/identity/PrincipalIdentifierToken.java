/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.io.Serializable;

/**
 * Requester Token for requests to/from an extension
 * Each user will have different token for different extension
 */
public class PrincipalIdentifierToken implements Serializable, NamedWriteable {
    public static final String NAME = "principal_identifier_token";
    private String token;

    public PrincipalIdentifierToken(String token) {
        this.token = token;
    }

    public String getToken() {
        return this.token;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(token);
    }
}
