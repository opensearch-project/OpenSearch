/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * InitializeExtensionRequest to initialize plugin
 *
 * @opensearch.internal
 */
public class InitializeExtensionSecurityRequest extends TransportRequest {

    private final String serviceAccountToken;

    public InitializeExtensionSecurityRequest(String serviceAccountToken) {
        this.serviceAccountToken = serviceAccountToken;
    }

    public InitializeExtensionSecurityRequest(StreamInput in) throws IOException {
        super(in);
        serviceAccountToken = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(serviceAccountToken);
    }

    public String getServiceAccountToken() {
        return serviceAccountToken;
    }

    @Override
    public String toString() {
        return "InitializeExtensionsRequest{" + "serviceAccountToken= " + serviceAccountToken + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitializeExtensionSecurityRequest that = (InitializeExtensionSecurityRequest) o;
        return Objects.equals(serviceAccountToken, that.serviceAccountToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAccountToken);
    }
}
