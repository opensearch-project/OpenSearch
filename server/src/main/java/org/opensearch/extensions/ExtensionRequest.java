/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.ExtensionRequestOuterClass;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * CLusterService Request for Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(ExtensionRequest.class);
    private final ExtensionRequestOuterClass.ExtensionRequest request;

    public ExtensionRequest(ExtensionRequestOuterClass.RequestType requestType) {
        this(requestType, null);
    }

    public ExtensionRequest(ExtensionRequestOuterClass.RequestType requestType, @Nullable String uniqueId) {
        ExtensionRequestOuterClass.ExtensionRequest.Builder builder = ExtensionRequestOuterClass.ExtensionRequest.newBuilder();
        if (uniqueId != null) {
            builder.setUniqiueId(uniqueId);
        }
        this.request = builder.setRequestType(requestType).build();
    }

    public ExtensionRequest(StreamInput in) throws IOException {
        super(in);
        this.request = ExtensionRequestOuterClass.ExtensionRequest.parseFrom(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    public ExtensionRequestOuterClass.RequestType getRequestType() {
        return this.request.getRequestType();
    }

    public String getUniqueId() {
        return request.getUniqiueId();
    }

    public String toString() {
        return "ExtensionRequest{" + request.toString() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionRequest that = (ExtensionRequest) o;
        return Objects.equals(request.getRequestType(), that.request.getRequestType())
            && Objects.equals(request.getUniqiueId(), that.request.getUniqiueId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getRequestType(), request.getUniqiueId());
    }
}
