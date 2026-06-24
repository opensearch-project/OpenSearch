/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class ActionConcurrencyLimitFilterTests extends OpenSearchTestCase {

    private static final Task TASK = null;
    private static final String SEARCH_ACTION = "indices:data/read/search";
    private static final String OTHER_ACTION = "indices:data/write/bulk";

    private static ClusterSettings clusterSettings(Settings settings) {
        Set<Setting<?>> all = new HashSet<>(ActionConcurrencyLimiterRegistry.ALL_SETTINGS);
        return new ClusterSettings(settings, all);
    }

    private static ActionConcurrencyLimiterRegistry registryFor(String alias, String actionName, String mode) {
        Settings s = Settings.builder()
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".action_name", actionName)
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".mode", mode)
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".warmup_duration", "0s")
            .build();
        return new ActionConcurrencyLimiterRegistry(
            s,
            clusterSettings(s),
            new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
        );
    }

    private static ActionConcurrencyLimiterRegistry limitedRegistry(String alias, String actionName, int limit) {
        Settings s = Settings.builder()
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".action_name", actionName)
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".mode", "enforced")
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".limit.initial", limit)
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".limit.max", limit)
            .put(ActionConcurrencyLimiterRegistry.SETTING_PREFIX + alias + ".warmup_duration", "0s")
            .build();
        return new ActionConcurrencyLimiterRegistry(
            s,
            clusterSettings(s),
            new org.opensearch.common.util.concurrent.ThreadContext(org.opensearch.common.settings.Settings.EMPTY)
        );
    }

    public void testPassesThroughUnlimitedAction() {
        ActionConcurrencyLimiterRegistry registry = registryFor("search", SEARCH_ACTION, "enforced");
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);
        AtomicBoolean proceeded = new AtomicBoolean(false);
        filter.apply(TASK, OTHER_ACTION, new SearchRequest(), null, noopListener(), passThroughChain(proceeded));
        assertTrue("should pass through unconfigured action", proceeded.get());
    }

    public void testPassesThroughWhenWithinLimit() {
        ActionConcurrencyLimiterRegistry registry = registryFor("search", SEARCH_ACTION, "enforced");
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);
        AtomicBoolean proceeded = new AtomicBoolean(false);
        filter.apply(TASK, SEARCH_ACTION, new SearchRequest(), null, noopListener(), passThroughChain(proceeded));
        assertTrue("should pass through when within limit", proceeded.get());
    }

    public void testRejectsWhenLimitFull() {
        ActionConcurrencyLimiterRegistry registry = limitedRegistry("search", SEARCH_ACTION, 1);
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);

        // fill the one slot
        filter.apply(TASK, SEARCH_ACTION, new SearchRequest(), null, noopListener(), passThroughChain(new AtomicBoolean()));

        // next should be rejected
        AtomicReference<Exception> captured = new AtomicReference<>();
        filter.apply(TASK, SEARCH_ACTION, new SearchRequest(), null, capturingFailureListener(captured), neverProceedChain());
        assertNotNull(captured.get());
        assertThat(captured.get(), org.hamcrest.Matchers.instanceOf(OpenSearchRejectedExecutionException.class));
    }

    public void testRejectIncrementsTotalRejected() {
        ActionConcurrencyLimiterRegistry registry = limitedRegistry("search", SEARCH_ACTION, 1);
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);

        filter.apply(TASK, SEARCH_ACTION, new SearchRequest(), null, noopListener(), passThroughChain(new AtomicBoolean()));
        filter.apply(TASK, SEARCH_ACTION, new SearchRequest(), null, noopListener(), neverProceedChain());

        assertEquals(1L, registry.getStats().getSnapshots().get(0).getTotalRejected());
    }

    public void testSuccessReleasesSlot() {
        ActionConcurrencyLimiterRegistry registry = registryFor("search", SEARCH_ACTION, "enforced");
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);
        AtomicReference<ActionListener<SearchResponse>> wrappedRef = new AtomicReference<>();

        filter.apply(
            TASK,
            SEARCH_ACTION,
            new SearchRequest(),
            null,
            noopListener(),
            (ActionFilterChain<SearchRequest, SearchResponse>) (t, a, req, listener) -> wrappedRef.set(listener)
        );

        assertEquals(1, registry.getStats().getSnapshots().get(0).getInFlight());
        wrappedRef.get().onResponse(mock(SearchResponse.class));
        assertEquals(0, registry.getStats().getSnapshots().get(0).getInFlight());
    }

    public void testFailureReleasesSlot() {
        ActionConcurrencyLimiterRegistry registry = registryFor("search", SEARCH_ACTION, "enforced");
        ActionConcurrencyLimitFilter filter = new ActionConcurrencyLimitFilter(registry);
        AtomicReference<ActionListener<SearchResponse>> wrappedRef = new AtomicReference<>();

        filter.apply(
            TASK,
            SEARCH_ACTION,
            new SearchRequest(),
            null,
            noopListener(),
            (ActionFilterChain<SearchRequest, SearchResponse>) (t, a, req, listener) -> wrappedRef.set(listener)
        );

        wrappedRef.get().onFailure(new RuntimeException("error"));
        assertEquals(0, registry.getStats().getSnapshots().get(0).getInFlight());
    }

    public void testOrderIsOne() {
        ActionConcurrencyLimiterRegistry registry = registryFor("search", SEARCH_ACTION, "disabled");
        assertEquals(1, new ActionConcurrencyLimitFilter(registry).order());
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static <Resp extends ActionResponse> ActionListener<Resp> noopListener() {
        return ActionListener.wrap(r -> {}, e -> {});
    }

    @SuppressWarnings("unchecked")
    private static <Resp extends ActionResponse> ActionListener<Resp> capturingFailureListener(AtomicReference<Exception> ref) {
        return ActionListener.wrap(r -> {}, ref::set);
    }

    @SuppressWarnings("unchecked")
    private static <Req extends ActionRequest, Resp extends ActionResponse> ActionFilterChain<Req, Resp> passThroughChain(
        AtomicBoolean proceeded
    ) {
        return (task, action, request, listener) -> proceeded.set(true);
    }

    @SuppressWarnings("unchecked")
    private static <Req extends ActionRequest, Resp extends ActionResponse> ActionFilterChain<Req, Resp> neverProceedChain() {
        return (task, action, request, listener) -> fail("chain.proceed should not have been called");
    }
}
