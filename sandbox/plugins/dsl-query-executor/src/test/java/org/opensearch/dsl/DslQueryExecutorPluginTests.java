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
import org.opensearch.dsl.action.DslValidateAction;
import org.opensearch.dsl.action.SearchActionFilter;
import org.opensearch.dsl.action.TransportDslExecuteAction;
import org.opensearch.dsl.action.TransportDslValidateAction;
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
        plugin.createComponents(mock(NodeClient.class), null, null, null, null, null, null, null, null, null, null);

        List<ActionFilter> filters = plugin.getActionFilters();
        assertEquals(1, filters.size());
        assertTrue(filters.get(0) instanceof SearchActionFilter);
    }

    public void testRegistersTransportAction() {
        var actions = plugin.getActions();

        assertEquals(2, actions.size());
        ActionPlugin.ActionHandler<?, ?> executeHandler = actions.get(0);
        assertEquals(DslExecuteAction.INSTANCE, executeHandler.getAction());
        assertEquals(TransportDslExecuteAction.class, executeHandler.getTransportAction());
        ActionPlugin.ActionHandler<?, ?> validateHandler = actions.get(1);
        assertEquals(DslValidateAction.INSTANCE, validateHandler.getAction());
        assertEquals(TransportDslValidateAction.class, validateHandler.getTransportAction());
    }
}
