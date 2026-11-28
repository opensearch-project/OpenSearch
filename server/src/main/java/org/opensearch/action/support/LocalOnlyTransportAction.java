/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.LocalOnlyActionRequest;
import org.opensearch.action.LocalOnlyActionResponse;
import org.opensearch.action.LocalOnlyActionType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.tasks.TaskManager;

/**
 * The local-only counterpart of {@link HandledTransportAction}. Executes through the normal
 * request validation, action-filter and task pipeline without registering a transport request handler.
 * Register it with an {@link org.opensearch.plugins.ActionPlugin.ActionHandler} for local node-client execution.
 *
 * <p>Local-only describes invocation, not trust: requests remain subject to authorization filters.
 * Execution uses the calling thread unless the implementation explicitly dispatches work elsewhere.
 * Subclasses must not register a transport handler for this action or forward its request to another node.
 *
 * @param <Request> the local request type
 * @param <Response> the local response type
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class LocalOnlyTransportAction<Request extends LocalOnlyActionRequest, Response extends LocalOnlyActionResponse> extends
    TransportAction<Request, Response> {

    /**
     * Creates a local-only action implementation using the existing action pipeline.
     * @param action the local-only action type
     * @param actionFilters the filters to run before execution
     * @param taskManager the node's task manager
     */
    protected LocalOnlyTransportAction(LocalOnlyActionType<Response> action, ActionFilters actionFilters, TaskManager taskManager) {
        super(action.name(), actionFilters, taskManager);
    }
}
