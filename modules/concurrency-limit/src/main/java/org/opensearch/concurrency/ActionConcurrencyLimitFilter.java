/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;

import java.util.Optional;

import com.netflix.concurrency.limits.Limiter;

/**
 * {@link ActionFilter} that enforces adaptive concurrency limits on any configured action.
 * <p>
 * Delegates to {@link ActionConcurrencyLimiterRegistry} for dispatch. If no limiter is
 * configured for the action, the request passes through unchanged. When the limit is
 * reached (after the warm-up period), the request is rejected with HTTP 429.
 * During warm-up, a no-op listener is returned so the limiter can calibrate without
 * rejecting real traffic — the filter still wraps the response listener, but the no-op
 * callbacks have no side effects.
 * <p>
 * The task and request are passed to {@link ActionConcurrencyLimiterRegistry#tryAcquire}
 * so that partition resolvers can inspect the request (e.g. read a header). A
 * {@link SearchRequestContext} is allocated only when the action's limiter is partitioned,
 * avoiding any per-request allocation on the non-partitioned hot path.
 */
public class ActionConcurrencyLimitFilter implements ActionFilter {

    private final ActionConcurrencyLimiterRegistry registry;

    /**
     * Creates a new filter backed by the given registry.
     *
     * @param registry the limiter registry
     */
    public ActionConcurrencyLimitFilter(ActionConcurrencyLimiterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (!registry.hasLimiterFor(action)) {
            chain.proceed(task, action, request, listener);
            return;
        }

        Optional<Limiter.Listener> token = registry.tryAcquire(action, task, request);
        if (token.isEmpty()) {
            listener.onFailure(
                new OpenSearchRejectedExecutionException("request rejected: concurrency limit reached for action [" + action + "]", true)
            );
            return;
        }

        Limiter.Listener limitToken = token.get();
        chain.proceed(task, action, request, new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                limitToken.onSuccess();
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof OpenSearchRejectedExecutionException) {
                    limitToken.onDropped();
                } else {
                    limitToken.onIgnore();
                }
                listener.onFailure(e);
            }
        });
    }
}
