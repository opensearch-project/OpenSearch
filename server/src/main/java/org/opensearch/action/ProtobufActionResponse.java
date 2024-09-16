/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action;

import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Base class for responses to action requests implemented by plugins.
*
* @opensearch.api
*/
public abstract class ProtobufActionResponse extends TransportResponse {

    public ProtobufActionResponse() {}

    public ProtobufActionResponse(byte[] in) throws IOException {
        super(in);
    }
}
