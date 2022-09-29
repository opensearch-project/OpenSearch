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
import java.util.Map;
import java.util.Objects;

/**
 * Request to execute REST actions on extension node
 *
 * @opensearch.internal
 */
public class RestExecuteOnExtensionRequest extends TransportRequest {

    private Method method;
    private String uri;
    private Map<String, String> params;
    private PrincipalIdentifierToken requestIssuerIdentity;

    public RestExecuteOnExtensionRequest(
        Method method,
        String uri,
        Map<String, String> params,
        PrincipalIdentifierToken requesterIdentifier
    ) {
        this.method = method;
        this.uri = uri;
        this.params = params;
        this.requestIssuerIdentity = requesterIdentifier;
    }

    public RestExecuteOnExtensionRequest(StreamInput in) throws IOException {
        super(in);
        method = in.readEnum(RestRequest.Method.class);
        uri = in.readString();
        params = in.readMap(StreamInput::readString, StreamInput::readString);
        requestIssuerIdentity = in.readNamedWriteable(PrincipalIdentifierToken.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(method);
        out.writeString(uri);
        out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        out.writeNamedWriteable(requestIssuerIdentity);
    }

    public Method getMethod() {
        return method;
    }

    public String getUri() {
        return uri;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public PrincipalIdentifierToken getRequestIssuerIdentity() {
        return requestIssuerIdentity;
    }

    @Override
    public String toString() {
        return "RestExecuteOnExtensionRequest{method="
            + method
            + ", uri="
            + uri
            + ", params="
            + params
            + ", requester="
            + requestIssuerIdentity.getToken()
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RestExecuteOnExtensionRequest that = (RestExecuteOnExtensionRequest) obj;
        return Objects.equals(method, that.method)
            && Objects.equals(uri, that.uri)
            && Objects.equals(params, that.params)
            && Objects.equals(requestIssuerIdentity, that.requestIssuerIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri, params, requestIssuerIdentity);
    }
}
