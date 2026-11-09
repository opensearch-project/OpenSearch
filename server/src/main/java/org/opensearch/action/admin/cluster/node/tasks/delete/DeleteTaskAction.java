/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.delete;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * ActionType for deleting a stored completed task result.
 *
 * @opensearch.internal
 */
public class DeleteTaskAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteTaskAction INSTANCE = new DeleteTaskAction();
    public static final String NAME = "cluster:admin/tasks/delete";

    private DeleteTaskAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
