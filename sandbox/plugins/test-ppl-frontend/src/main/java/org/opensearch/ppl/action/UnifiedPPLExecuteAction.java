/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.action.ActionType;

/**
 * Action singleton for unified PPL query execution.
 * Registered as a transport action in TestPPLPlugin.
 */
public class UnifiedPPLExecuteAction extends ActionType<PPLResponse> {
    public static final String NAME = "cluster:internal/qe/unified_ppl_execute";
    public static final UnifiedPPLExecuteAction INSTANCE = new UnifiedPPLExecuteAction();

    private UnifiedPPLExecuteAction() {
        super(NAME, PPLResponse::new);
    }
}
