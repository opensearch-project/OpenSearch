/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.grok.Grok;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.List;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 1, scope = OpenSearchIntegTestCase.Scope.TEST)
public class LocalOnlyGrokActionIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(IngestCommonModulePlugin.class, RejectSortedGrokPlugin.class);
    }

    public void testLocalExecutionUsesActionFiltersWithoutTransportHandler() {
        assertNotNull(internalCluster().getInstance(DynamicActionRegistry.class).get(GrokProcessorGetAction.INSTANCE));
        assertNull(internalCluster().getInstance(TransportService.class).getRequestHandler(GrokProcessorGetAction.NAME));

        var response = client().execute(GrokProcessorGetAction.INSTANCE, new GrokProcessorGetAction.Request(false)).actionGet();
        assertEquals(Grok.BUILTIN_PATTERNS, response.getGrokPatterns());

        var exception = expectThrows(
            OpenSearchSecurityException.class,
            () -> client().execute(GrokProcessorGetAction.INSTANCE, new GrokProcessorGetAction.Request(true)).actionGet()
        );
        assertEquals("Rejected by the action filter", exception.getMessage());
    }

    public static class RejectSortedGrokPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter.Simple() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    if (GrokProcessorGetAction.NAME.equals(action) && ((GrokProcessorGetAction.Request) request).sorted()) {
                        listener.onFailure(new OpenSearchSecurityException("Rejected by the action filter"));
                        return false;
                    }
                    return true;
                }
            });
        }
    }
}
