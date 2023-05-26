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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.extensions.proto.ExtensionIdentityProto;
import org.opensearch.extensions.proto.ExtensionRequestProto;
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
    private final ExtensionRequestProto.ExtensionRequest request;

    public ExtensionRequest(ExtensionRequestProto.RequestType requestType) {
        this(requestType, null);
    }

    public ExtensionRequest(ExtensionRequestProto.RequestType requestType, @Nullable String uniqueId) {
        ExtensionRequestProto.ExtensionRequest.Builder builder = ExtensionRequestProto.ExtensionRequest.newBuilder();
        if (uniqueId != null) {
            builder.setIdentity(ExtensionIdentityProto.ExtensionIdentity.newBuilder().setUniqueId(uniqueId).build());
        }
        this.request = builder.setRequestType(requestType).build();
    }

    public ExtensionRequest(StreamInput in) throws IOException {
        super(in);
        this.request = ExtensionRequestProto.ExtensionRequest.parseFrom(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByteArray(request.toByteArray());
    }

    public ExtensionRequestProto.RequestType getRequestType() {
        return this.request.getRequestType();
    }

    public String getUniqueId() {
        return request.getIdentity().getUniqueId();
    }

    public String toString() {
        return "ExtensionRequest{" + request.toString() + '}';
    }

    public ExtensionIdentityProto.ExtensionIdentity getExtensionIdentity() {
        return request.getIdentity();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionRequest that = (ExtensionRequest) o;
        return Objects.equals(request.getRequestType(), that.request.getRequestType())
            && Objects.equals(request.getIdentity().getUniqueId(), that.request.getIdentity().getUniqueId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(request.getRequestType(), request.getIdentity().getUniqueId());
    }
}
