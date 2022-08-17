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
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;
import java.security.Principal;

/**
 * CLusterService Request for Extensibility
 *
 * @opensearch.internal
 */
public class ExtensionRequest extends TransportRequest {
    private static final Logger logger = LogManager.getLogger(ExtensionRequest.class);
    private ExtensionsOrchestrator.RequestType requestType;
    private Principal user;

    public ExtensionRequest(ExtensionsOrchestrator.RequestType requestType, Principal user) {
        this.requestType = requestType;
        this.user = user;
    }

    public ExtensionRequest(StreamInput in) throws IOException {
        super(in);
        this.requestType = in.readEnum(ExtensionsOrchestrator.RequestType.class);
        this.user = (Principal)in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestType);
        out.writeGenericValue(user);
    }

    public ExtensionsOrchestrator.RequestType getRequestType() {
        return this.requestType;
    }

    public Principal getUser() {
        return this.user;
    }

    public String toString() {
        return "ExtensionRequest{" + "requestType=" + getRequestType() + ", user=" + getUser() + "}";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionRequest that = (ExtensionRequest) o;
        return Objects.equals(getRequestType(), that.getRequestType()) && Objects.equals(getUser(), that.getUser());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRequestType(), getUser());
    }

}
