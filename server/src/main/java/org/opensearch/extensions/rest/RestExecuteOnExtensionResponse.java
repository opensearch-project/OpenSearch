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
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Response to execute REST Actions on the extension node. Wraps the components of a {@link RestResponse}.
 *
 * @opensearch.internal
 */
public class RestExecuteOnExtensionResponse extends TransportResponse {
    private RestStatus status;
    private String contentType;
    private byte[] content;
    private Map<String, List<String>> headers;

    /**
     * Instantiate this object with a status and response string.
     *
     * @param status  The REST status.
     * @param responseString  The response content as a String.
     */
    public RestExecuteOnExtensionResponse(RestStatus status, String responseString) {
        this(status, BytesRestResponse.TEXT_CONTENT_TYPE, responseString.getBytes(StandardCharsets.UTF_8), Collections.emptyMap());
    }

    /**
     * Instantiate this object with the components of a {@link RestResponse}.
     *
     * @param status  The REST status.
     * @param contentType  The type of the content.
     * @param content  The content.
     * @param headers  The headers.
     */
    public RestExecuteOnExtensionResponse(RestStatus status, String contentType, byte[] content, Map<String, List<String>> headers) {
        setStatus(status);
        setContentType(contentType);
        setContent(content);
        setHeaders(headers);
    }

    /**
     * Instantiate this object from a Transport Stream
     *
     * @param in  The stream input.
     * @throws IOException on transport failure.
     */
    public RestExecuteOnExtensionResponse(StreamInput in) throws IOException {
        setStatus(RestStatus.readFrom(in));
        setContentType(in.readString());
        setContent(in.readByteArray());
        setHeaders(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        RestStatus.writeTo(out, status);
        out.writeString(contentType);
        out.writeByteArray(content);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public RestStatus getStatus() {
        return status;
    }

    public void setStatus(RestStatus status) {
        this.status = status;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, List<String>> headers) {
        this.headers = Map.copyOf(headers);
    }
}
