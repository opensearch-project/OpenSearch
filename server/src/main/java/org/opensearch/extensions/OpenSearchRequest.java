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

/**
 * Request from OpenSearch to an Extension
 *
 * @opensearch.internal
 */
public class OpenSearchRequest extends TransportRequest {

    private static final Logger logger = LogManager.getLogger(OpenSearchRequest.class);
    private ExtensionsManager.OpenSearchRequestType requestType;

    /**
     * @param requestType String identifying the default extension point to invoke on the extension
     */
    public OpenSearchRequest(ExtensionsManager.OpenSearchRequestType requestType) {
        this.requestType = requestType;
    }

    /**
     * @param in StreamInput from which a string identifying the default extension point to invoke on the extension is read from
     */
    public OpenSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.requestType = in.readEnum(ExtensionsManager.OpenSearchRequestType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestType);
    }

    @Override
    public String toString() {
        return "OpenSearchRequest{" + "requestType=" + requestType + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenSearchRequest that = (OpenSearchRequest) o;
        return Objects.equals(requestType, that.requestType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType);
    }

    public ExtensionsManager.OpenSearchRequestType getRequestType() {
        return this.requestType;
    }

}
