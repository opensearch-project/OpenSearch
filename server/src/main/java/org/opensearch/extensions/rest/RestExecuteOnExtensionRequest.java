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
import org.opensearch.identity.ExtensionIdentifier;
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

    // TODO: this signature should be revisited encryption/decryption is implemented
    private String requesterToken;

    public RestExecuteOnExtensionRequest(Method method, String uri, ExtensionIdentifier extensionIdentifier) {
        this.method = method;
        this.uri = uri;
        this.requesterToken = extensionIdentifier.generateToken();
    }

    public RestExecuteOnExtensionRequest(StreamInput in) throws IOException {
        super(in);
        try {
            method = RestRequest.Method.valueOf(in.readString());
        } catch (IllegalArgumentException e) {
            throw new IOException(e);
        }
        uri = in.readString();
        requesterToken = in.readString(); // more complex than this
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(method.name());
        out.writeString(uri);
        out.writeString(requesterToken);
    }

    public Method getMethod() {
        return method;
    }

    public String getUri() {
        return uri;
    }

    public String getRequesterToken() {
        return requesterToken;
    }

    @Override
    public String toString() {
        return "RestExecuteOnExtensionRequest{method=" + method + ", uri=" + uri + ", requester = " + requesterToken + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RestExecuteOnExtensionRequest that = (RestExecuteOnExtensionRequest) obj;
        return Objects.equals(method, that.method) && Objects.equals(uri, that.uri) && Objects.equals(requesterToken, that.requesterToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri, requesterToken);
    }
}
