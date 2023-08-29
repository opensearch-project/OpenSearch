/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

public class IssueServiceAccountRequest extends TransportRequest {

    private final String serviceAccountToken;

    public IssueServiceAccountRequest(String serviceAccountToken) {
        this.serviceAccountToken = serviceAccountToken;
    }

    public IssueServiceAccountRequest(StreamInput in) throws IOException {
        super(in);
        this.serviceAccountToken = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    public String getServiceAccountToken() {
        return this.serviceAccountToken;
    }

    @Override
    public String toString() {
        return "IssueServiceAccountRequest {" + "serviceAccountToken=" + serviceAccountToken + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IssueServiceAccountRequest that = (IssueServiceAccountRequest) o;
        return Objects.equals(serviceAccountToken, that.serviceAccountToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAccountToken);
    }
}
