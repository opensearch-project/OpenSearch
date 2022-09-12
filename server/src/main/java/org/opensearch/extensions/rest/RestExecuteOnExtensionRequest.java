/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.identity.PrincipalIdentifierToken;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to execute REST actions on extension node
 *
 * @opensearch.internal
 */
public class RestExecuteOnExtensionRequest extends TransportRequest {

    private Method method;
    private String uri;

    private PrincipalIdentifierToken token;

    public RestExecuteOnExtensionRequest(Method method, String uri, PrincipalIdentifierToken token) {
        this.method = method;
        this.uri = uri;
        this.token = token;
    }

    public RestExecuteOnExtensionRequest(StreamInput in) throws IOException {
        super(in);
        try {
            method = RestRequest.Method.valueOf(in.readString());
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        }
        uri = in.readString();
        token = in.readNamedWriteable(PrincipalIdentifierToken.class, PrincipalIdentifierToken.NAME);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(method.name());
        out.writeString(uri);
        out.writeNamedWriteable(token);
    }

    public Method getMethod() {
        return method;
    }

    public String getUri() {
        return uri;
    }

    public PrincipalIdentifierToken getToken() {
        return token;
    }

    @Override
    public String toString() {
        return "RestExecuteOnExtensionRequest{method=" + method + ", uri=" + uri + ", requester = " + token.getToken() + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RestExecuteOnExtensionRequest that = (RestExecuteOnExtensionRequest) obj;
        return Objects.equals(method, that.method) && Objects.equals(uri, that.uri) && Objects.equals(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri, token);
    }
}
