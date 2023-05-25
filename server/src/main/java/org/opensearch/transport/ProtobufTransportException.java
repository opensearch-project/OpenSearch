/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import com.google.protobuf.CodedInputStream;
import org.opensearch.ProtobufOpenSearchException;

import java.io.IOException;

/**
 * Thrown for any transport errors
*
* @opensearch.internal
*/
public class ProtobufTransportException extends ProtobufOpenSearchException {
    public ProtobufTransportException(Throwable cause) {
        super(cause);
    }

    public ProtobufTransportException(CodedInputStream in) throws IOException {
        super(in);
    }

    public ProtobufTransportException(String msg) {
        super(msg);
    }

    public ProtobufTransportException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
