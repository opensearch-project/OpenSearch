/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status;

import org.opensearch.action.ActionType;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;

/** Action type for retrieving tiering status of all indices in the cluster. */
public class ListTieringStatusAction extends ActionType<ListTieringStatusResponse> {

    /** Singleton instance. */
    public static final ListTieringStatusAction INSTANCE = new ListTieringStatusAction();
    /** Action name for listing tiering status. */
    public static final String NAME = "cluster:admin/_tier/all";

    private ListTieringStatusAction() {
        super(NAME, ListTieringStatusResponse::new);
    }
}
