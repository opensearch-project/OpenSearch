/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to put a search pipeline
 *
 * @opensearch.internal
 */
public class PutSearchPipelineRequest extends AcknowledgedRequest<PutSearchPipelineRequest> implements ToXContentObject {
    private String id;
    private BytesReference source;
    private XContentType xContentType;

    public PutSearchPipelineRequest(String id, BytesReference source, MediaType mediaType) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        if (mediaType instanceof XContentType == false) {
            throw new IllegalArgumentException(
                PutSearchPipelineRequest.class.getSimpleName() + " found unsupported media type [" + mediaType.getClass().getName() + "]"
            );
        }
        this.xContentType = XContentType.fromMediaType(Objects.requireNonNull(mediaType));
    }

    public PutSearchPipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        source = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBytesReference(source);
        out.writeEnum(xContentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source != null) {
            builder.rawValue(source.streamInput(), xContentType);
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }

}
