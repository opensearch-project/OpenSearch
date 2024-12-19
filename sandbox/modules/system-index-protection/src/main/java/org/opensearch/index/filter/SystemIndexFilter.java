/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.filter;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.indices.SystemIndexRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class SystemIndexFilter implements ActionFilter {

    private final IndexResolverReplacer indexResolverReplacer;
    private final WildcardMatcher deniedActionsMatcher;
    private final ThreadPool threadPool;

    public SystemIndexFilter(final IndexResolverReplacer indexResolverReplacer, final ThreadPool threadPool) {
        this.indexResolverReplacer = indexResolverReplacer;
        this.threadPool = threadPool;

        final List<String> deniedActionPatternsList = deniedActionPatterns();

        deniedActionsMatcher = WildcardMatcher.from(deniedActionPatternsList);
    }

    private static List<String> deniedActionPatterns() {
        final List<String> systemIndexDeniedActionPatternsList = new ArrayList<>();
        systemIndexDeniedActionPatternsList.add("indices:data/write*");
        systemIndexDeniedActionPatternsList.add("indices:admin/delete*");
        systemIndexDeniedActionPatternsList.add("indices:admin/mapping/delete*");
        systemIndexDeniedActionPatternsList.add("indices:admin/mapping/put*");
        systemIndexDeniedActionPatternsList.add("indices:admin/freeze*");
        systemIndexDeniedActionPatternsList.add("indices:admin/settings/update*");
        systemIndexDeniedActionPatternsList.add("indices:admin/aliases");
        systemIndexDeniedActionPatternsList.add("indices:admin/close*");
        systemIndexDeniedActionPatternsList.add("cluster:admin/snapshot/restore*");
        return systemIndexDeniedActionPatternsList;
    }

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (threadPool.getThreadContext().isSystemContext()) {
            chain.proceed(task, action, request, listener);
            return;
        }
        if (deniedActionsMatcher.test(action)) {
            final IndexResolverReplacer.Resolved resolved = indexResolverReplacer.resolveRequest(request);
            final Set<String> allIndices = resolved.getAllIndices();
            Set<String> matchingSystemIndices = SystemIndexRegistry.matchesSystemIndexPattern(allIndices);
            if (!matchingSystemIndices.isEmpty()) {
                String err = String.format(Locale.ROOT, "Cannot perform %s on matching system indices %s", action, matchingSystemIndices);
                listener.onFailure(new OpenSearchSecurityException(err, RestStatus.FORBIDDEN));
                return;
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
