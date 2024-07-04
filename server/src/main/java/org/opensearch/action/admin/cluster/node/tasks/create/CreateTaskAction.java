/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.create;

import org.opensearch.action.ActionType;

/**
 * Transport action for creating new task.
 *
 * @opensearch.internal
 */
public class CreateTaskAction extends ActionType<CreateTaskResponse> {
    public static final CreateTaskAction INSTANCE = new CreateTaskAction();
    public static final String NAME = "cluster:monitor/create/task";

    private CreateTaskAction() {
        super(NAME, CreateTaskResponse::new);
    }
}
