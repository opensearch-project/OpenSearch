/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action;

import com.google.protobuf.CodedInputStream;
import org.opensearch.transport.ProtobufTransportResponse;

import java.io.IOException;

/**
 * Base class for responses to action requests implemented by plugins.
*
* @opensearch.api
*/
public abstract class ProtobufActionResponse extends ProtobufTransportResponse {

    public ProtobufActionResponse() {}

    public ProtobufActionResponse(CodedInputStream in) throws IOException {
        super(in);
    }
}
