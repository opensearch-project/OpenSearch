/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.executioncontextplugin;

import org.opensearch.action.ActionType;

/**
 * Test action to get the name of the plugin executing transport actions
 */
public class TestGetExecutionContextAction extends ActionType<TestGetExecutionContextResponse> {
    /**
     * Get execution context action instance
     */
    public static final TestGetExecutionContextAction INSTANCE = new TestGetExecutionContextAction();
    /**
     * Get execution context action name
     */
    public static final String NAME = "cluster:admin/executioncontext";

    private TestGetExecutionContextAction() {
        super(NAME, TestGetExecutionContextResponse::new);
    }
}
