/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * ClusterService Request for Action Listener onFailure
 *
 * @opensearch.internal
 */
public class ExtensionActionListenerOnFailureRequest extends TransportRequest {
    private String failureExceptionMessage;

    /**
     *  Instantiates a request for ActionListener onFailure from an extension
     *
     * @param failureExceptionMessage A String that contains both the Exception type and message
     */
    public ExtensionActionListenerOnFailureRequest(String failureExceptionMessage) {
        super();
        this.failureExceptionMessage = failureExceptionMessage;
    }

    public ExtensionActionListenerOnFailureRequest(StreamInput in) throws IOException {
        super(in);
        this.failureExceptionMessage = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(failureExceptionMessage);
    }

    public String toString() {
        return "ExtensionActionListenerOnFailureRequest{" + "failureExceptionMessage= " + failureExceptionMessage + " }";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionActionListenerOnFailureRequest that = (ExtensionActionListenerOnFailureRequest) o;
        return Objects.equals(failureExceptionMessage, that.failureExceptionMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failureExceptionMessage);
    }

    public String getFailureException() {
        return failureExceptionMessage;
    }

}
