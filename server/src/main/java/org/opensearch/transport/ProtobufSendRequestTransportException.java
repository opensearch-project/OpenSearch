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
import org.opensearch.cluster.node.ProtobufDiscoveryNode;

import java.io.IOException;

/**
 * Thrown when an error occurs while sending a request
*
* @opensearch.internal
*/
public class ProtobufSendRequestTransportException extends ProtobufActionTransportException implements OpenSearchWrapperException {

    public ProtobufSendRequestTransportException(ProtobufDiscoveryNode node, String action, Throwable cause) {
        super(node == null ? null : node.getName(), node == null ? null : node.getAddress(), action, cause);
    }

    public ProtobufSendRequestTransportException(CodedInputStream in) throws IOException {
        super(in);
    }
}
