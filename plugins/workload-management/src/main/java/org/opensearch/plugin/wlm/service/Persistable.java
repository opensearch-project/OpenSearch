/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.service;

import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.UpdateQueryGroupRequest;
import org.opensearch.plugin.wlm.UpdateQueryGroupResponse;

/**
 * This interface defines the key APIs for implementing QueryGroup persistence
 */
public interface Persistable<T> {
    /**
     * update the QueryGroup in a durable storage
     * @param updateQueryGroupRequest - the updateQueryGroupRequest
     * @param listener - ActionListener for UpdateQueryGroupRequest
     */
    void update(UpdateQueryGroupRequest updateQueryGroupRequest, ActionListener<UpdateQueryGroupResponse> listener);
}
