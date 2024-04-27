/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.resource_limit_group.CreateResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.DeleteResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.GetResourceLimitGroupResponse;
import org.opensearch.plugin.resource_limit_group.UpdateResourceLimitGroupResponse;

/**
 * This interface defines the key APIs for implementing resource limit group persistence
 */
public interface Persistable<T> {

    // void persist(ResourceLimitGroup resourceLimitGroup, ActionListener<CreateResourceLimitGroupResponse> listener);

    /**
     * persists the resource limit group in a durable storage
     * @param resourceLimitGroup
     * @param listener
     */
    void persist(T resourceLimitGroup, ActionListener<CreateResourceLimitGroupResponse> listener);

    /**
     * update the resource limit group in a durable storage
     * @param resourceLimitGroup
     * @param listener
     */
    void update(T resourceLimitGroup, ActionListener<UpdateResourceLimitGroupResponse> listener);

    /**
     * fetch the resource limit group in a durable storage
     * @param name
     * @param listener
     */
    void get(String name, ActionListener<GetResourceLimitGroupResponse> listener);

    /**
     * delete the resource limit group in a durable storage
     * @param name
     * @param listener
     */
    void delete(String name, ActionListener<DeleteResourceLimitGroupResponse> listener);
}
