/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.transport.TransportRequest;
import org.opensearch.http.HttpRequest;

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
    private String uri;
    private String path;
    private Map<String, String> params;
    private Map<String, List<String>> headers;
    private MediaType mediaType = null;
    private BytesReference content;
    // The owner of this request object
    // Will be replaced with PrincipalIdentifierToken class from feature/identity
    private String principalIdentifierToken;
    private HttpRequest.HttpVersion httpVersion;

    // Tracks consumed parameters and content
    private final Set<String> consumedParams = new HashSet<>();
    private boolean contentConsumed = false;

    /**
     * This object can be instantiated given method, uri, params, content and identifier
     *
     * @param method              of type {@link Method}
     * @param uri                 the REST uri string (excluding the query)
     * @param path                the REST path
     * @param params              the REST params
     * @param headers             the REST headers
     * @param mediaType           the content type, or null for plain text or no content
     * @param content             the REST request content
     * @param principalIdentifier the owner of this request
     * @param httpVersion         the REST HTTP protocol version
     */
    public ExtensionRestRequest(
        Method method,
        String uri,
        String path,
        Map<String, String> params,
        Map<String, List<String>> headers,
        MediaType mediaType,
        BytesReference content,
        String principalIdentifier,
        HttpRequest.HttpVersion httpVersion
    ) {
        this.method = method;
        this.uri = uri;
        this.path = path;
        this.params = params;
        this.headers = headers;
        this.mediaType = mediaType;
        this.content = content;
        this.principalIdentifierToken = principalIdentifier;
        this.httpVersion = httpVersion;
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
        uri = in.readString();
        path = in.readString();
        params = in.readMap(StreamInput::readString, StreamInput::readString);
        headers = in.readMap(StreamInput::readString, StreamInput::readStringList);
        if (in.readBoolean()) {
            if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
                mediaType = in.readMediaType();
            } else {
                mediaType = in.readEnum(XContentType.class);
            }
        }
        content = in.readBytesReference();
        principalIdentifierToken = in.readString();
        httpVersion = in.readEnum(HttpRequest.HttpVersion.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(method);
        out.writeString(uri);
        out.writeString(path);
        out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeStringCollection);
        out.writeBoolean(mediaType != null);
        if (mediaType != null) {
            if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
                mediaType.writeTo(out);
            } else {
                out.writeEnum((XContentType) mediaType);
            }
        }
        out.writeBytesReference(content);
        out.writeString(principalIdentifierToken);
        out.writeEnum(httpVersion);
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
     * Gets the REST uri
     *
     * @return This REST request's uri
     */
    public String uri() {
        return uri;
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
     * Gets the headers of request
     * @return a map of request headers
     */
    public Map<String, List<String>> headers() {
        return headers;
    }

    /**
     * Gets the content type, if any.
     *
     * @return the content type of the {@link #content()}, or null if the context is plain text or if there is no content.
     */
    public MediaType getXContentType() {
        return mediaType;
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

    /**
     * @return This REST request's HTTP protocol version
     */
    public HttpRequest.HttpVersion protocolVersion() {
        return httpVersion;
    }

    @Override
    public String toString() {
        return "ExtensionRestRequest{method="
            + method
            + ", uri="
            + uri
            + ", path="
            + path
            + ", params="
            + params
            + ", headers="
            + headers.toString()
            + ", xContentType="
            + mediaType
            + ", contentLength="
            + content.length()
            + ", requester="
            + principalIdentifierToken
            + ", httpVersion="
            + httpVersion
            + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionRestRequest that = (ExtensionRestRequest) obj;
        return Objects.equals(method, that.method)
            && Objects.equals(uri, that.uri)
            && Objects.equals(path, that.path)
            && Objects.equals(params, that.params)
            && Objects.equals(headers, that.headers)
            && Objects.equals(mediaType, that.mediaType)
            && Objects.equals(content, that.content)
            && Objects.equals(principalIdentifierToken, that.principalIdentifierToken)
            && Objects.equals(httpVersion, that.httpVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, uri, path, params, headers, mediaType, content, principalIdentifierToken, httpVersion);
    }
}
