/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

/**
 * Action filter that limits system index interaction for plugins
 *
 * Plugins are passed a PluginAwareNodeClient that allows a plugin to switch context and securely
 * interact with system indices it registers through SystemIndexPlugin.getSystemIndexDescriptors
 */
public class SystemIndexFilter implements ActionFilter {

    private final ThreadContext threadContext;
    private final IndexNameExpressionResolver resolver;

    public SystemIndexFilter(ThreadContext threadContext, IndexNameExpressionResolver resolver) {
        this.threadContext = threadContext;
        this.resolver = resolver;
    }

    @Override
    public int order() {
        return Integer.MIN_VALUE + 100;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        // String pluginExecutionContext = threadContext.getHeader(ThreadContext.PLUGIN_EXECUTION_CONTEXT);
        // if (pluginExecutionContext != null) {
        // IndexResolverReplacer.Resolved requestedResolved = indexResolverReplacer.resolveRequest(request);
        // if (!requestedResolved.getAllIndices().isEmpty()) {
        // Set<String> matchingSystemIndices = SystemIndexRegistry.matchesPluginSystemIndexPattern(
        // pluginExecutionContext,
        // requestedResolved.getAllIndices()
        // );
        // if (!matchingSystemIndices.equals(requestedResolved.getAllIndices())) {
        // String err = "Plugin " + pluginExecutionContext + " can only interact with its own system indices";
        // listener.onFailure(new OpenSearchSecurityException(err, RestStatus.FORBIDDEN));
        // return;
        // }
        // }
        // }
        // TODO Figure out how to resolve Request -> Indices
        chain.proceed(task, action, request, listener);
    }
}
