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
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
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
    private List<String> consumedParams;
    private boolean contentConsumed;

    /**
     * Instantiate this object with the components of a {@link RestResponse}.
     *
     * @param status  The REST status.
     * @param contentType  The type of the content.
     * @param content  The content.
     * @param headers  The headers.
     * @param consumedParams  The consumed params.
     * @param contentConsumed  Whether content was consumed.
     */
    public RestExecuteOnExtensionResponse(
        RestStatus status,
        String contentType,
        byte[] content,
        Map<String, List<String>> headers,
        List<String> consumedParams,
        boolean contentConsumed
    ) {
        super();
        setStatus(status);
        setContentType(contentType);
        setContent(content);
        setHeaders(headers);
        setConsumedParams(consumedParams);
        setContentConsumed(contentConsumed);
    }

    /**
     * Instantiate this object from a Transport Stream.
     *
     * @param in  The stream input.
     * @throws IOException on transport failure.
     */
    public RestExecuteOnExtensionResponse(StreamInput in) throws IOException {
        setStatus(in.readEnum(RestStatus.class));
        setContentType(in.readString());
        setContent(in.readByteArray());
        setHeaders(in.readMapOfLists(StreamInput::readString, StreamInput::readString));
        setConsumedParams(in.readStringList());
        setContentConsumed(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeString(contentType);
        out.writeByteArray(content);
        out.writeMapOfLists(headers, StreamOutput::writeString, StreamOutput::writeString);
        out.writeStringCollection(consumedParams);
        out.writeBoolean(contentConsumed);
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

    public List<String> getConsumedParams() {
        return consumedParams;
    }

    public void setConsumedParams(List<String> consumedParams) {
        this.consumedParams = consumedParams;
    }

    public boolean isContentConsumed() {
        return contentConsumed;
    }

    public void setContentConsumed(boolean contentConsumed) {
        this.contentConsumed = contentConsumed;
    }
}
