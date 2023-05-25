/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.OpenSearchWrapperException;
import org.opensearch.common.transport.ProtobufTransportAddress;

import java.io.IOException;

/**
 * A remote exception for an action. A wrapper exception around the actual remote cause and does not fill the
* stack trace.
*
* @opensearch.internal
*/
public class ProtobufRemoteTransportException extends ProtobufActionTransportException implements OpenSearchWrapperException {

    public ProtobufRemoteTransportException(String msg, Throwable cause) {
        super(msg, null, null, cause);
    }

    public ProtobufRemoteTransportException(String name, ProtobufTransportAddress address, String action, Throwable cause) {
        super(name, address, action, cause);
    }

    public ProtobufRemoteTransportException(CodedInputStream in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // no need for stack trace here, we always have cause
        return this;
    }
}
