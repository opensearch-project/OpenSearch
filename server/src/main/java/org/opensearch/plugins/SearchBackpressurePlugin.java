/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.search.backpressure.SearchBackpressureCancellationListener;

import java.util.Collections;
import java.util.List;

/**
 * An extension point for plugins to register listeners for search backpressure events.
 * Plugins implementing this interface can receive notifications when tasks are cancelled
 * due to resource usage violations.
 *
 * @opensearch.api
 */
public interface SearchBackpressurePlugin {

    /**
     * Returns a list of listeners to be registered with the SearchBackpressureService.
     * These listeners will be notified when tasks are cancelled due to resource pressure.
     *
     * @return a list of cancellation listeners, empty list if none
     */
    default List<SearchBackpressureCancellationListener> getSearchBackpressureCancellationListeners() {
        return Collections.emptyList();
    }
}
