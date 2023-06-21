/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.main;

import com.google.protobuf.CodedInputStream;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Transport request for main action
*
* @opensearch.internal
*/
public class ProtobufMainRequest extends ProtobufActionRequest {

    public ProtobufMainRequest() {}

    ProtobufMainRequest(CodedInputStream in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
