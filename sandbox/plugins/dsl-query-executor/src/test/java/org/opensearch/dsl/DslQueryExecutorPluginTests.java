/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.dsl.action.DslExecuteAction;
import org.opensearch.dsl.action.SearchActionFilter;
import org.opensearch.dsl.action.TransportDslExecuteAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.mockito.Mockito.mock;

public class DslQueryExecutorPluginTests extends OpenSearchTestCase {

    private DslQueryExecutorPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new DslQueryExecutorPlugin();
    }

    public void testGetActionFiltersEmptyBeforeCreateComponents() {
        List<ActionFilter> filters = plugin.getActionFilters();

        assertTrue(filters.isEmpty());
    }

    public void testGetActionFiltersAfterCreateComponents() {
        plugin.createComponents(
            mock(NodeClient.class),
            null, null, null, null,
            null, null, null,
            null, null, null
        );

        List<ActionFilter> filters = plugin.getActionFilters();
        assertEquals(1, filters.size());
        assertTrue(filters.get(0) instanceof SearchActionFilter);
    }

    public void testRegistersTransportAction() {
        var actions = plugin.getActions();

        assertEquals(1, actions.size());
        ActionPlugin.ActionHandler<?, ?> handler = actions.get(0);
        assertEquals(DslExecuteAction.INSTANCE, handler.getAction());
        assertEquals(TransportDslExecuteAction.class, handler.getTransportAction());
    }
}
