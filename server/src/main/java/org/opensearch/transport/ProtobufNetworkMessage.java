/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.util.concurrent.ThreadContext;

/**
 * Represents a transport message sent over the network. Subclasses implement serialization and
* deserialization.
*
* @opensearch.internal
*/
public abstract class ProtobufNetworkMessage {

    protected final Version version;
    protected final ProtobufWriteable threadContext;
    protected final long requestId;
    protected final byte status;

    ProtobufNetworkMessage(ThreadContext threadContext, Version version, byte status, long requestId) {
        this.threadContext = (ProtobufWriteable) threadContext.captureAsProtobufWriteable();
        this.version = version;
        this.requestId = requestId;
        this.status = status;
    }

    public Version getVersion() {
        return version;
    }

    public long getRequestId() {
        return requestId;
    }

    boolean isCompress() {
        return TransportStatus.isCompress(status);
    }

    boolean isResponse() {
        return TransportStatus.isRequest(status) == false;
    }

    boolean isRequest() {
        return TransportStatus.isRequest(status);
    }

    boolean isHandshake() {
        return TransportStatus.isHandshake(status);
    }

    boolean isError() {
        return TransportStatus.isError(status);
    }
}
