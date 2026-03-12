/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.ppl.action.TestPPLTransportAction;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;

/**
 * Example front-end plugin using analytics-engine.
 * The {@code QueryPlanExecutor} and {@code SchemaProvider}
 * are received by {@link TestPPLTransportAction} via Guice injection.
 */
public class TestPPLPlugin extends Plugin implements ActionPlugin, ExtensiblePlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(UnifiedPPLExecuteAction.INSTANCE, TestPPLTransportAction.class));
    }
}
