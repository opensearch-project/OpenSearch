/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.node;

import com.google.protobuf.CodedInputStream;
import org.opensearch.ProtobufOpenSearchException;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;

import java.io.IOException;

/**
 * An exception indicating that node is closed.
*
* @opensearch.internal
*/
public class ProtobufNodeClosedException extends ProtobufOpenSearchException {

    public ProtobufNodeClosedException(ProtobufDiscoveryNode node) {
        super("node closed " + node);
    }

    public ProtobufNodeClosedException(CodedInputStream in) throws IOException {
        super(in);
    }
}
