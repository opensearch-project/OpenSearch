/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * An action invoked through the local node client, not through a transport request handler.
 * Implementations extend {@link org.opensearch.action.support.TransportAction} directly and
 * remain subject to its validation, action filters and task tracking. Local-only does not mean trusted.
 *
 * @param <Response> the local response type
 * @opensearch.experimental
 */
@ExperimentalApi
public class LocalOnlyActionType<Response extends LocalOnlyActionResponse> extends ActionType<Response> {
    /**
     * Creates a local-only action with its normal, authorizable action name.
     * @param name the unique action name
     */
    public LocalOnlyActionType(String name) {
        super(name, in -> { throw new UnsupportedOperationException("Action [" + name + "] is local-only"); });
    }
}
