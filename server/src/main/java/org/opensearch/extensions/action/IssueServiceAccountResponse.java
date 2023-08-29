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
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

public class IssueServiceAccountResponse extends TransportResponse {

    private String name;
    private String serviceAccountString;

    public IssueServiceAccountResponse(String name, String serviceAccountString) {
        this.name = name;
        this.serviceAccountString = serviceAccountString;
    }

    public IssueServiceAccountResponse(StreamInput in) throws IOException {
        this.name = in.readString();
        this.serviceAccountString = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(serviceAccountString);
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */

    public String getName() {
        return this.name;
    }

    public String getServiceAccountString() {
        return this.serviceAccountString;
    }

    @Override
    public String toString() {
        return "InitializeExtensionResponse{" + "name = " + name + " , " + "received service account = " + serviceAccountString + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IssueServiceAccountResponse that = (IssueServiceAccountResponse) o;
        return Objects.equals(name, that.name) && Objects.equals(serviceAccountString, that.serviceAccountString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, serviceAccountString);
    }

}
