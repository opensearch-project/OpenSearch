/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.ingest;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * transport request to put a pipeline
 *
 * @opensearch.internal
 */
public class PutPipelineRequest extends AcknowledgedRequest<PutPipelineRequest> implements ToXContentObject {

    private String id;
    private BytesReference source;
    private XContentType xContentType;

    /**
     * Create a new pipeline request with the id and source along with the content type of the source
     *
     * @deprecated use {@link #PutPipelineRequest(String, BytesReference, MediaType)} instead
     */
    @Deprecated
    public PutPipelineRequest(String id, BytesReference source, XContentType xContentType) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    /**
     * Create a new pipeline request with the id and source along with the content type of the source
     */
    public PutPipelineRequest(String id, BytesReference source, MediaType mediaType) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        if (mediaType instanceof XContentType == false) {
            throw new IllegalArgumentException("PutPipelineRequest found unsupported media type [" + mediaType.getClass().getName() + "]");
        }
        this.xContentType = XContentType.fromMediaType(Objects.requireNonNull(mediaType));
    }

    public PutPipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        source = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
    }

    PutPipelineRequest() {}

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
