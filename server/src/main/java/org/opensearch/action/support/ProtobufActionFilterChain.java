/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.tasks.ProtobufTask;

/**
 * A filter chain allowing to continue and process the transport action request
*
* @opensearch.internal
*/
public interface ProtobufActionFilterChain<Request extends ProtobufActionRequest, Response extends ProtobufActionResponse> {

    /**
     * Continue processing the request. Should only be called if a response has not been sent through
    * the given {@link ActionListener listener}
    */
    void proceed(ProtobufTask task, String action, Request request, ActionListener<Response> listener);
}
