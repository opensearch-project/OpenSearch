/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.qg.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.qg.CreateQueryGroupResponse;
import org.opensearch.plugin.qg.DeleteQueryGroupResponse;
import org.opensearch.plugin.qg.GetQueryGroupResponse;
import org.opensearch.plugin.qg.UpdateQueryGroupRequest;
import org.opensearch.plugin.qg.UpdateQueryGroupResponse;

/**
 * This interface defines the key APIs for implementing QueruGroup persistence
 */
public interface Persistable<T> {

    /**
     * persists the QueryGroup in a durable storage
     * @param queryGroup
     * @param listener
     */
    void persist(T queryGroup, ActionListener<CreateQueryGroupResponse> listener);

    /**
     * update the QueryGroup in a durable storage
     * @param updateQueryGroupRequest
     * @param listener
     */
    void update(UpdateQueryGroupRequest updateQueryGroupRequest, ActionListener<UpdateQueryGroupResponse> listener);

    /**
     * fetch the QueryGroup in a durable storage
     * @param name
     * @param listener
     */
    void get(String name, ActionListener<GetQueryGroupResponse> listener);

    /**
     * delete the QueryGroup in a durable storage
     * @param name
     * @param listener
     */
    void delete(String name, ActionListener<DeleteQueryGroupResponse> listener);
}
