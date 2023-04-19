/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when expected crypto client is not found.
 *
 * @opensearch.internal
 */
public class CryptoClientMissingException extends OpenSearchException {
    private final String name;
    private final String type;

    public CryptoClientMissingException(String clientName, String clientType) {
        super("[Crypto client : " + clientName + " of type " + clientType + " ] is missing");
        this.name = clientName;
        this.type = clientType;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    public CryptoClientMissingException(StreamInput in) throws IOException {
        super(in);
        this.name = in.readOptionalString();
        this.type = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeOptionalString(type);
    }
}
