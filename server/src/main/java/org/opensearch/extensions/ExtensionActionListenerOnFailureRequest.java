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

import java.io.IOException;
import java.util.Objects;

/**
 * CLusterService Request for Action Listener onFailure
 *
 * @opensearch.internal
 */
public class ExtensionActionListenerOnFailureRequest extends ExtensionRequest {
    private static final Logger logger = LogManager.getLogger(ExtensionRequest.class);
    private ExtensionsOrchestrator.RequestType requestType;
    private String failureException;

    public ExtensionActionListenerOnFailureRequest(ExtensionsOrchestrator.RequestType requestType, String failureException) {
        super(requestType);
        this.failureException = failureException;
    }

    public ExtensionActionListenerOnFailureRequest(StreamInput in) throws IOException {
        super(in);
        this.failureException = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(failureException);
    }

    public ExtensionsOrchestrator.RequestType getRequestType() {
        return this.requestType;
    }

    public String toString() {
        return "ExtensionRequest{" + "requestType=" + requestType + ", failureException=" + "}";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtensionActionListenerOnFailureRequest that = (ExtensionActionListenerOnFailureRequest) o;
        return Objects.equals(requestType, that.requestType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType);
    }

    public String getFailureException() {
        return failureException;
    }

}
