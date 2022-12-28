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

/**
 * CLusterService Request for Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(ExtensionRequest.class);
    private final ExtensionsManager.RequestType requestType;
    private final String uniqueId;

    public ExtensionRequest(ExtensionsManager.RequestType requestType, String uniqueId) {
        this.requestType = requestType;
        this.uniqueId = uniqueId;
    }

    public ExtensionRequest(StreamInput in) throws IOException {
        super(in);
        this.requestType = in.readEnum(ExtensionsManager.RequestType.class);
        this.uniqueId = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestType);
        out.writeOptionalString(uniqueId);
    }

    public ExtensionsManager.RequestType getRequestType() {
        return this.requestType;
    }

    @Nullable
    public String getUniqueId() {
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
        if (uniqueId == null) {
            return Objects.equals(requestType, that.requestType) && true;
        } else if (that.uniqueId == null) {
            return Objects.equals(requestType, that.requestType) && false;
        } else {
            return Objects.equals(requestType, that.requestType) && uniqueId.equals(that.uniqueId);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType, uniqueId);
    }
}
