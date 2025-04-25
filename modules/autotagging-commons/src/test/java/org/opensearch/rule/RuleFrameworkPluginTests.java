/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.rest.RestGetRuleAction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;

public class RuleFrameworkPluginTests extends OpenSearchTestCase {
    RuleFrameworkPlugin plugin = new RuleFrameworkPlugin();;

    public void testGetActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> handlers = plugin.getActions();
        assertEquals(1, handlers.size());
        assertEquals(GetRuleAction.INSTANCE.name(), handlers.get(0).getAction().name());
    }

    public void testGetRestHandlers() {
        Settings settings = Settings.EMPTY;
        RestHandler handler = plugin.getRestHandlers(
            settings,
            mock(org.opensearch.rest.RestController.class),
            null,
            null,
            null,
            mock(IndexNameExpressionResolver.class),
            () -> mock(DiscoveryNodes.class)
        ).get(0);

        assertTrue(handler instanceof RestGetRuleAction);
    }
}
