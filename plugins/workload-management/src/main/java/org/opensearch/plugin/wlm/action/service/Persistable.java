/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.action.CreateQueryGroupResponse;

/**
 * This interface defines the key APIs for implementing QueruGroup persistence
 */
public interface Persistable<T> {

    /**
     * persists the QueryGroup in a durable storage
     * @param queryGroup - queryGroup to be persisted
     * @param listener - ActionListener for CreateQueryGroupResponse
     */
    void persist(T queryGroup, ActionListener<CreateQueryGroupResponse> listener);
}
