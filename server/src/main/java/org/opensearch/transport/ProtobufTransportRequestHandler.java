/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.Task;

/**
 * Handles transport requests
*
* @opensearch.internal
*/
public interface ProtobufTransportRequestHandler<T extends ProtobufTransportRequest> {

    void messageReceived(T request, TransportChannel channel, ProtobufTask task) throws Exception;
}
