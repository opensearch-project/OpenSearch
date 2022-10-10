/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.common.io.stream.NamedWriteable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.crypto.SecretKey;

/**
 * Requester Token for requests to/from an extension
 * Each user will have different token for different extension
 *
 * @opensearch.internal
 */
public class PrincipalIdentifierToken implements NamedWriteable {
    public static final String NAME = "principal_identifier_token";
    private final String token;
   
    
    /**
     * Should only be instantiated via extensionTokenProcessor.generateToken(..)
     * @param token string value of token
     */
    protected PrincipalIdentifierToken(String token) {
        this.token = token;
   
    }

    public PrincipalIdentifierToken(StreamInput in) throws IOException {
        this.token = in.readString();
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

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof PrincipalIdentifierToken)) return false;
        PrincipalIdentifierToken other = (PrincipalIdentifierToken) obj;
        return Objects.equals(token, other.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(token);
    }
}
