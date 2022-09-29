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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Request to execute REST actions on extension node
 *
 * @opensearch.api
 */
public class ExtensionRestRequest extends TransportRequest {

    private Method method;
    private String uri;
    private Map<String, String> params;
    // The owner of this request object
    private PrincipalIdentifierToken principalIdentifierToken;
    // Tracks consumed parameters
    private final Set<String> consumedParams = new HashSet<>();

    /**
     * This object can be instantiated given method, uri, params and identifier
     * @param method of type {@link Method}
     * @param uri url string
     * @param params the REST params
     * @param principalIdentifier the owner of this request
     */
    public ExtensionRestRequest(Method method, String uri, Map<String, String> params, PrincipalIdentifierToken principalIdentifier) {
        this.method = method;
        this.uri = uri;
        this.params = params;
        this.principalIdentifierToken = principalIdentifier;
    }

    /**
     * Instantiate this request from input stream
     * @param in Input stream
     * @throws IOException on failure to read the stream
     */
    public ExtensionRestRequest(StreamInput in) throws IOException {
        super(in);
        method = in.readEnum(RestRequest.Method.class);
        uri = in.readString();
        params = in.readMap(StreamInput::readString, StreamInput::readString);
        principalIdentifierToken = in.readNamedWriteable(PrincipalIdentifierToken.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(method);
        out.writeString(uri);
        out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        out.writeNamedWriteable(principalIdentifierToken);
    }

    /**
     * @return This REST request {@link Method} type
     */
    public Method method() {
        return method;
    }

    /**
     * @return This REST request's uri
     */
    public String uri() {
        return uri;
    }

    /**
     * Gets the value of a parameter, consuming it in the process.
     * @param key The parameter key
     * @return The parameter value if it exists, or null.
     */
    public String param(String key) {
        consumedParams.add(key);
        return params.get(key);
    }

    /**
     * Gets the value of a parameter, consuming it in the process.
     * @param key The parameter key
     * @param defaultValue A value to return if the parameter value doesn't exist.
     * @return The parameter value if it exists, or the default value.
     */
    public String param(String key, String defaultValue) {
        consumedParams.add(key);
        return params.getOrDefault(key, defaultValue);
    }

    /**
     * Gets the full map of params without consuming them.
     * Rest Handlers should use {@link #param(String)} or {@link #param(String, String)} to get parameter values.
     *
     * @return This REST request's params
     */
    public Map<String, String> params() {
        return params;
    }

    /**
     * Returns parameters consumed by {@link #param(String)} or {@link #param(String, String)}.
     *
     * @return a list of consumed parameters.
     */
    public List<String> consumedParams() {
        return new ArrayList<>(consumedParams);
    }

    /**
     * @return This REST request issuer's identity token
     */
    public PrincipalIdentifierToken getRequestIssuerIdentity() {
        return principalIdentifierToken;
    }

    @Override
    public String toString() {
        return "ExtensionRestRequest{method="
            + method
            + ", uri="
            + uri
            + ", params="
            + params
            + ", requester="
            + principalIdentifierToken.getToken()
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionRestRequest that = (ExtensionRestRequest) obj;
        return Objects.equals(method, that.method)
            && Objects.equals(uri, that.uri)
            && Objects.equals(params, that.params)
            && Objects.equals(principalIdentifierToken, that.principalIdentifierToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri, params, principalIdentifierToken);
    }
}
