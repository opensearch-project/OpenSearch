/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;

import java.io.IOException;

/**
 * Base exception for a failed node
*
* @opensearch.internal
*/
public class ProtobufOpenSearchException extends RuntimeException implements ProtobufWriteable {

    private String message;

    public ProtobufOpenSearchException(String message) {
        super(message);
        this.message = message;
    }

    public ProtobufOpenSearchException(CodedInputStream in) throws IOException {
        super(in.readString());
        this.message = in.readString();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeStringNoTag(this.getMessage());
    }

    public String getMessage() {
        return this.message;
    }

}
