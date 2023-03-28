/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
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
 * Request to execute REST actions on extension node.
 * This contains necessary portions of a {@link RestRequest} object, but does not pass the full request for security concerns.
 *
 * @opensearch.api
 */
public class ExtensionRestRequest extends TransportRequest {

    private Method method;
    private String path;
    private Map<String, String> params;
    private XContentType xContentType = null;
    private BytesReference content;
    // The owner of this request object
    // Will be replaced with PrincipalIdentifierToken class from feature/identity
    private String principalIdentifierToken;

    // Tracks consumed parameters and content
    private final Set<String> consumedParams = new HashSet<>();
    private boolean contentConsumed = false;

    /**
     * This object can be instantiated given method, uri, params, content and identifier
     *
     * @param method of type {@link Method}
     * @param path the REST path string (excluding the query)
     * @param params the REST params
     * @param xContentType the content type, or null for plain text or no content
     * @param content the REST request content
     * @param principalIdentifier the owner of this request
     */
    public ExtensionRestRequest(
        Method method,
        String path,
        Map<String, String> params,
        XContentType xContentType,
        BytesReference content,
        String principalIdentifier
    ) {
        this.method = method;
        this.path = path;
        this.params = params;
        this.xContentType = xContentType;
        this.content = content;
        this.principalIdentifierToken = principalIdentifier;
    }

    /**
     * Instantiate this request from input stream
     *
     * @param in Input stream
     * @throws IOException on failure to read the stream
     */
    public ExtensionRestRequest(StreamInput in) throws IOException {
        super(in);
        method = in.readEnum(RestRequest.Method.class);
        path = in.readString();
        params = in.readMap(StreamInput::readString, StreamInput::readString);
        if (in.readBoolean()) {
            xContentType = in.readEnum(XContentType.class);
        }
        content = in.readBytesReference();
        principalIdentifierToken = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(method);
        out.writeString(path);
        out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        out.writeBoolean(xContentType != null);
        if (xContentType != null) {
            out.writeEnum(xContentType);
        }
        out.writeBytesReference(content);
        out.writeString(principalIdentifierToken);
    }

    /**
     * Gets the REST method
     *
     * @return This REST request {@link Method} type
     */
    public Method method() {
        return method;
    }

    /**
     * Gets the REST path
     *
     * @return This REST request's path
     */
    public String path() {
        return path;
    }

    /**
     * Gets the full map of params without consuming them. Rest Handlers should use {@link #param(String)} or {@link #param(String, String)}
     * to get parameter values.
     *
     * @return This REST request's params
     */
    public Map<String, String> params() {
        return params;
    }

    /**
     * Tests whether a parameter named {@code key} exists.
     *
     * @param key The parameter to test.
     * @return True if there is a value for this parameter.
     */
    public boolean hasParam(String key) {
        return params.containsKey(key);
    }

    /**
     * Gets the value of a parameter, consuming it in the process.
     *
     * @param key The parameter key
     * @return The parameter value if it exists, or null.
     */
    public String param(String key) {
        consumedParams.add(key);
        return params.get(key);
    }

    /**
     * Gets the value of a parameter, consuming it in the process.
     *
     * @param key The parameter key
     * @param defaultValue  A value to return if the parameter value doesn't exist.
     * @return The parameter value if it exists, or the default value.
     */
    public String param(String key, String defaultValue) {
        consumedParams.add(key);
        return params.getOrDefault(key, defaultValue);
    }

    /**
     * Gets the value of a parameter as a long, consuming it in the process.
     *
     * @param key The parameter key
     * @param defaultValue A value to return if the parameter value doesn't exist.
     * @return The parameter value if it exists, or the default value.
     */
    public long paramAsLong(String key, long defaultValue) {
        String value = param(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unable to parse param '" + key + "' value '" + value + "' to a long.", e);
        }
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
     * Gets the content type, if any.
     *
     * @return the content type of the {@link #content()}, or null if the context is plain text or if there is no content.
     */
    public XContentType getXContentType() {
        return xContentType;
    }

    /**
     * Gets the content.
     *
     * @return This REST request's content
     */
    public BytesReference content() {
        contentConsumed = true;
        return content;
    }

    /**
     * Tests whether content exists.
     *
     * @return True if there is non-empty content.
     */
    public boolean hasContent() {
        return content.length() > 0;
    }

    /**
     * Tests whether content has been consumed.
     *
     * @return True if the content was consumed.
     */
    public boolean isContentConsumed() {
        return contentConsumed;
    }

    /**
     * Gets a parser for the contents of this request if there is content and an xContentType.
     *
     * @param xContentRegistry The extension's xContentRegistry
     * @return A parser for the given content and content type.
     * @throws OpenSearchParseException on missing body or xContentType.
     * @throws IOException on a failure creating the parser.
     */
    public final XContentParser contentParser(NamedXContentRegistry xContentRegistry) throws IOException {
        if (!hasContent() || getXContentType() == null) {
            throw new OpenSearchParseException("There is no request body or the ContentType is invalid.");
        }
        return getXContentType().xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, content.streamInput());
    }

    /**
     * @return This REST request issuer's identity token
     */
    public String getRequestIssuerIdentity() {
        return principalIdentifierToken;
    }

    @Override
    public String toString() {
        return "ExtensionRestRequest{method="
            + method
            + ", path="
            + path
            + ", params="
            + params
            + ", xContentType="
            + xContentType
            + ", contentLength="
            + content.length()
            + ", requester="
            + principalIdentifierToken
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionRestRequest that = (ExtensionRestRequest) obj;
        return Objects.equals(method, that.method)
            && Objects.equals(path, that.path)
            && Objects.equals(params, that.params)
            && Objects.equals(xContentType, that.xContentType)
            && Objects.equals(content, that.content)
            && Objects.equals(principalIdentifierToken, that.principalIdentifierToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, path, params, xContentType, content, principalIdentifierToken);
    }
}
