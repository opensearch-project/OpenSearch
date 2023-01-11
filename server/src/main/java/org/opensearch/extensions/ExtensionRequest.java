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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * CLusterService Request for Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(ExtensionRequest.class);
    private final ExtensionsManager.RequestType requestType;
    private final Optional<String> uniqueId;

    public ExtensionRequest(ExtensionsManager.RequestType requestType) {
        this(requestType, null);
    }

    public ExtensionRequest(ExtensionsManager.RequestType requestType, @Nullable String uniqueId) {
        this.requestType = requestType;
        this.uniqueId = uniqueId == null ? Optional.empty() : Optional.of(uniqueId);
    }

    public ExtensionRequest(StreamInput in) throws IOException {
        super(in);
        this.requestType = in.readEnum(ExtensionsManager.RequestType.class);
        String id = in.readOptionalString();
        this.uniqueId = id == null ? Optional.empty() : Optional.of(id);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestType);
        out.writeOptionalString(uniqueId.orElse(null));
    }

    public ExtensionsManager.RequestType getRequestType() {
        return this.requestType;
    }

    public Optional<String> getUniqueId() {
        return uniqueId;
    }

    public String toString() {
        return "ExtensionRequest{" + "requestType=" + requestType + "uniqueId=" + uniqueId + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionRequest that = (ExtensionRequest) o;
        return Objects.equals(requestType, that.requestType) && Objects.equals(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType, uniqueId);
    }
}
