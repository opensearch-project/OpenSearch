/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.action.DeleteQueryGroupResponse;

/**
 * This interface defines the key APIs for implementing QueruGroup persistence
 */
public interface Persistable<T> {
    /**
     * delete the QueryGroup in a durable storage
     * @param name - QueryGroup name to be deleted
     * @param listener - ActionListener of DeleteQueryGroupResponse
     */
    void delete(String name, ActionListener<DeleteQueryGroupResponse> listener);
}
