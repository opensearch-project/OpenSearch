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
 * Request from OpenSearch to an Extension to invoke a default extension point
 *
 * @opensearch.internal
 */
public class DefaultExtensionPointRequest extends TransportRequest {

    private static final Logger logger = LogManager.getLogger(DefaultExtensionPointRequest.class);
    private String extensionPoint;

    /**
     * @param extensionPoint string identifying the default extension point to invoke on the extension
     */
    public DefaultExtensionPointRequest(String extensionPoint) {
        this.extensionPoint = extensionPoint;
    }

    /**
     * @param in StreamInput from which a string identifying the default extension point to invoke on the extension is read from
     */
    public DefaultExtensionPointRequest(StreamInput in) throws IOException {
        super(in);
        this.extensionPoint = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(extensionPoint);
    }

    @Override
    public String toString() {
        return "DefaultExtensionPointRequest{" + "extensionPoint=" + extensionPoint + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultExtensionPointRequest that = (DefaultExtensionPointRequest) o;
        return Objects.equals(extensionPoint, that.extensionPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensionPoint);
    }

}
