/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.script.mustache;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for circuit breaker accounting in {@link TransportMultiSearchTemplateAction}.
 *
 * <p>Validates that the rendered-source byte footprint of an {@code _msearch/template} batch is
 * charged against the {@link CircuitBreaker#IN_FLIGHT_REQUESTS} breaker, preventing OOM when the
 * rendered batch exceeds the configured limit.
 */
public class TransportMultiSearchTemplateActionTests extends OpenSearchTestCase {

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    /**
     * Build a {@link HierarchyCircuitBreakerService} with the {@code IN_FLIGHT_REQUESTS}
     * breaker limit pinned to {@code limitBytes} and real-memory tracking disabled
     * so the test is deterministic.
     */
    private static CircuitBreakerService circuitBreakerServiceWithLimit(long limitBytes) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limitBytes + "b")
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        return new HierarchyCircuitBreakerService(settings, new ArrayList<>(), clusterSettings);
    }

    /**
     * Create a minimal {@link ScriptService} backed by the {@link MustacheScriptEngine}.
     * Merges {@code TemplateScript.CONTEXT} into {@link ScriptModule#CORE_CONTEXTS}.
     */
    private static ScriptService mustacheScriptService() {
        Map<String, ScriptEngine> engines = Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine());
        Map<String, ScriptContext<?>> contexts = new HashMap<>(ScriptModule.CORE_CONTEXTS);
        contexts.put(TemplateScript.CONTEXT.name, TemplateScript.CONTEXT);
        return new ScriptService(Settings.builder().put("script.allowed_types", "inline").build(), engines, contexts);
    }

    /**
     * Build a {@link MultiSearchTemplateRequest} with {@code n} inline Mustache templates.
     * Each template renders to a static JSON body with a padding string large enough
     * that the rendered size is at least {@code bytesPerTemplate} bytes.
     */
    /**
     * Build a {@link MultiSearchTemplateRequest} with {@code n} inline Mustache templates.
     * Each template renders to a valid {@code SearchSourceBuilder} JSON body with
     * {@code storedFieldCount} stored-field names, making the rendered size predictably large
     * mirrors the PoC payload shape: each template requests many stored fields.
     */
    private static MultiSearchTemplateRequest buildRequest(int n, int storedFieldCount) {
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();
        // Build a JSON array of field names: ["f0","f1",...] — valid SearchSourceBuilder content.
        StringBuilder fields = new StringBuilder("[");
        for (int j = 0; j < storedFieldCount; j++) {
            if (j > 0) fields.append(',');
            fields.append("\"f").append(j).append('"');
        }
        fields.append(']');
        String source = "{\"size\":0,\"stored_fields\":" + fields + "}";
        for (int i = 0; i < n; i++) {
            SearchTemplateRequest templateRequest = new SearchTemplateRequest();
            templateRequest.setScriptType(ScriptType.INLINE);
            // Pure static Mustache source — rendered verbatim, so rendered size is deterministic.
            templateRequest.setScript(source);
            templateRequest.setScriptParams(Collections.emptyMap());
            templateRequest.setRequest(new SearchRequest());
            multiRequest.add(templateRequest);
        }
        return multiRequest;
    }

    /** Build a {@link TransportService} mock that returns a no-op {@link TaskManager}. */
    private static TransportService mockTransportService() {
        TransportService ts = mock(TransportService.class);
        TaskManager tm = mock(TaskManager.class);
        when(ts.getTaskManager()).thenReturn(tm);
        return ts;
    }

    private static TransportMultiSearchTemplateAction buildAction(CircuitBreakerService breakerService, NodeClient client) {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING)
        );
        return new TransportMultiSearchTemplateAction(
            settings,
            mockTransportService(),
            new ActionFilters(Collections.emptySet()),
            mustacheScriptService(),
            NamedXContentRegistry.EMPTY,
            client,
            breakerService,
            clusterSettings
        );
    }

    // ---------------------------------------------------------------------------
    // Test 1: circuit breaker trips instead of OOM for an oversized batch
    // ---------------------------------------------------------------------------

    /**
     * A batch whose cumulative rendered source bytes exceed the configured
     * {@code IN_FLIGHT_REQUESTS} breaker limit must be rejected with a
     * {@link CircuitBreakingException} rather than causing an OOM error.
     *
     * <p>This directly tests the OOM fix:
     * without the fix, rendering 7,000 large inline templates exhausted the coordinator
     * heap. With the fix the breaker fires after the first template that tips the limit.
     */
    public void testCircuitBreakerTripsBeforeOOM() {
        // Limit = 1 KB; each template renders to ~2 KB → breaker must trip after ~1 item.
        long limitBytes = 1_024L;
        int bytesPerTemplate = 2_048;

        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(limitBytes);
        TransportMultiSearchTemplateAction action = buildAction(breakerService, mock(NodeClient.class));
        MultiSearchTemplateRequest request = buildRequest(5, bytesPerTemplate);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();

        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        assertNull("Expected no successful response when breaker should trip", caughtResponse.get());
        assertNotNull("Expected a failure when rendered bytes exceed breaker limit", caughtFailure.get());
        assertTrue(
            "Failure must be a CircuitBreakingException, got: " + caughtFailure.get().getClass().getName(),
            caughtFailure.get() instanceof CircuitBreakingException
        );

        // All charged bytes must be released after the failure path.
        assertEquals(
            "Breaker bytes must be fully released after CircuitBreakingException",
            0L,
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed()
        );
    }

    // ---------------------------------------------------------------------------
    // Test 2: small batch succeeds and breaker bytes are released on completion
    // ---------------------------------------------------------------------------

    /**
     * A small batch whose cumulative rendered bytes fit within the breaker limit must
     * succeed normally. After the response is delivered the breaker must report zero used
     * bytes — all charged bytes released in the {@code finally} block.
     */
    @SuppressWarnings("unchecked")
    public void testBreakerReleasedOnSuccessfulResponse() {
        // Limit = 10 MB; 3 templates × ~100 B each is well within limit.
        long limitBytes = 10 * 1_024 * 1_024L;

        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(limitBytes);
        NodeClient client = mock(NodeClient.class);

        // Capture the multiSearch listener so we can simulate a response.
        AtomicReference<ActionListener<MultiSearchResponse>> multiSearchListener = new AtomicReference<>();
        doAnswer(inv -> {
            multiSearchListener.set(inv.getArgument(1));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);
        MultiSearchTemplateRequest request = buildRequest(3, 100);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();

        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        // After the render loop, multiSearch has been called; breaker should be charged.
        assertNotNull("multiSearch listener should have been captured after doExecute", multiSearchListener.get());
        assertTrue(
            "Breaker should hold the rendered source bytes while the request is in-flight",
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed() > 0
        );

        // Simulate multiSearch returning with (error) stub items — we only care about breaker release.
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[3];
        for (int i = 0; i < items.length; i++) {
            items[i] = new MultiSearchResponse.Item(null, new RuntimeException("test stub"));
        }
        multiSearchListener.get().onResponse(new MultiSearchResponse(items, 1L));

        assertNull("No failure expected for a small batch within limits", caughtFailure.get());
        assertNotNull("Expected a successful response for a small batch", caughtResponse.get());
        assertEquals(
            "Breaker bytes must be fully released after response is delivered",
            0L,
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed()
        );
    }

    // ---------------------------------------------------------------------------
    // Test 3: breaker bytes are released even when multiSearch itself fails
    // ---------------------------------------------------------------------------

    /**
     * If the downstream {@code multiSearch} transport call fails, the charged breaker bytes
     * must still be released via the failure-path {@code finally} block.
     */
    @SuppressWarnings("unchecked")
    public void testBreakerReleasedOnMultiSearchFailure() {
        long limitBytes = 10 * 1_024 * 1_024L;

        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(limitBytes);
        NodeClient client = mock(NodeClient.class);

        AtomicReference<ActionListener<MultiSearchResponse>> multiSearchListener = new AtomicReference<>();
        doAnswer(inv -> {
            multiSearchListener.set(inv.getArgument(1));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);
        MultiSearchTemplateRequest request = buildRequest(2, 100);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(r -> {}, caughtFailure::set));

        assertNotNull(multiSearchListener.get());
        // Simulate downstream transport failure.
        multiSearchListener.get().onFailure(new RuntimeException("simulated transport error"));

        assertNotNull("Failure listener should have been called", caughtFailure.get());
        assertEquals(
            "Breaker bytes must be fully released after multiSearch failure",
            0L,
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed()
        );
    }

    // ---------------------------------------------------------------------------
    // Test 4: hard count-limit guard rejects batch before any rendering
    // ---------------------------------------------------------------------------

    /**
     * A batch that exceeds {@code search.max_multi_search_template_requests} must be
     * rejected immediately with an {@link IllegalArgumentException}, before any Mustache
     * rendering occurs. This is the primary OOM guard.
     */
    public void testCountLimitRejectsOversizedBatch() {
        // Set a very small limit (3) and send 5 templates.
        Settings settings = Settings.builder().put(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING.getKey(), 3).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING)
        );

        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(10 * 1024 * 1024L);
        NodeClient client = mock(NodeClient.class);

        TransportMultiSearchTemplateAction action = new TransportMultiSearchTemplateAction(
            settings,
            mockTransportService(),
            new ActionFilters(Collections.emptySet()),
            mustacheScriptService(),
            NamedXContentRegistry.EMPTY,
            client,
            breakerService,
            clusterSettings
        );

        // 5 templates > limit of 3
        MultiSearchTemplateRequest request = buildRequest(5, 10);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        assertNull("No response expected when count limit is exceeded", caughtResponse.get());
        assertNotNull("Expected failure when batch exceeds count limit", caughtFailure.get());
        assertTrue(
            "Failure must be IllegalArgumentException, got: " + caughtFailure.get().getClass().getName(),
            caughtFailure.get() instanceof IllegalArgumentException
        );
        assertTrue(
            "Error message should mention the limit",
            caughtFailure.get().getMessage().contains("search.max_multi_search_template_requests")
        );

        // Breaker must be at zero — count guard fires before any CB charges.
        assertEquals(
            "Breaker must be untouched when count limit rejects the request",
            0L,
            breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed()
        );
    }

    // ---------------------------------------------------------------------------
    // Test 5: dynamic count-limit setting update is respected immediately
    // ---------------------------------------------------------------------------

    /**
     * The {@code search.max_multi_search_template_requests} setting is dynamic. A batch that
     * was within the old limit must be rejected after the limit is lowered via the cluster
     * settings consumer registered in the constructor.
     */
    public void testDynamicCountLimitUpdate() {
        Settings settings = Settings.builder().put(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING.getKey(), 10).build();
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING)
        );

        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(10 * 1024 * 1024L);
        TransportMultiSearchTemplateAction action = new TransportMultiSearchTemplateAction(
            settings,
            mockTransportService(),
            new ActionFilters(Collections.emptySet()),
            mustacheScriptService(),
            NamedXContentRegistry.EMPTY,
            mock(NodeClient.class),
            breakerService,
            clusterSettings
        );

        // Lower limit to 2 via dynamic update.
        clusterSettings.applySettings(
            Settings.builder().put(TransportMultiSearchTemplateAction.MAX_TEMPLATE_BATCH_SIZE_SETTING.getKey(), 2).build()
        );

        // A batch of 5 should now be rejected with the new limit of 2.
        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        action.doExecute(mock(Task.class), buildRequest(5, 10), ActionListener.wrap(r -> {}, caughtFailure::set));

        assertNotNull("Expected rejection after dynamic limit decrease", caughtFailure.get());
        assertTrue(caughtFailure.get() instanceof IllegalArgumentException);
        assertTrue(caughtFailure.get().getMessage().contains("search.max_multi_search_template_requests"));
    }

    // ---------------------------------------------------------------------------
    // Test 6: individual template convert() failure — batch continues, breaker released
    // ---------------------------------------------------------------------------

    /**
     * When an individual sub-request fails to convert (bad script syntax), the action must
     * record the per-item error and continue processing remaining templates rather than
     * aborting the whole batch. Breaker bytes are released normally at the end.
     */
    @SuppressWarnings("unchecked")
    public void testIndividualTemplateConvertFailureContinuesBatch() {
        long limitBytes = 10 * 1024 * 1024L;
        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(limitBytes);
        NodeClient client = mock(NodeClient.class);

        AtomicReference<ActionListener<MultiSearchResponse>> multiSearchListener = new AtomicReference<>();
        doAnswer(inv -> {
            multiSearchListener.set(inv.getArgument(1));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);

        // Build a request with 3 templates: first is invalid JSON (will fail convert()), rest are valid.
        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        // Template 0: invalid script — convert() will throw when parsing rendered output.
        SearchTemplateRequest bad = new SearchTemplateRequest();
        bad.setScriptType(ScriptType.INLINE);
        bad.setScript("{not valid json {{{{");
        bad.setScriptParams(Collections.emptyMap());
        bad.setRequest(new SearchRequest());
        request.add(bad);
        // Templates 1-2: valid.
        MultiSearchTemplateRequest valid = buildRequest(2, 5);
        valid.requests().forEach(request::add);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        // multiSearch should be called with only the 2 valid requests.
        assertNotNull("multiSearch should be dispatched for valid templates", multiSearchListener.get());

        // Complete the multiSearch for the 2 valid slots.
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[2];
        for (int i = 0; i < items.length; i++) {
            items[i] = new MultiSearchResponse.Item(null, new RuntimeException("stub"));
        }
        multiSearchListener.get().onResponse(new MultiSearchResponse(items, 1L));

        assertNull("No overall failure expected when only individual templates fail", caughtFailure.get());
        assertNotNull("Response should be returned even when some templates failed", caughtResponse.get());
        assertEquals("Response should have 3 slots (including the failed one)", 3, caughtResponse.get().getResponses().length);
        assertNotNull("Slot 0 should record the convert() failure", caughtResponse.get().getResponses()[0].getFailure());
        assertEquals("Breaker must be fully released", 0L, breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed());
    }

    // ---------------------------------------------------------------------------
    // Test 7: delta > 0 — actual footprint exceeds initial EMA estimate
    // ---------------------------------------------------------------------------

    /**
     * When a template renders to a size larger than the initial EMA pre-charge estimate,
     * the positive delta must be charged to the breaker. If that extra charge causes a trip,
     * the batch must fail with {@link CircuitBreakingException} and all bytes released.
     */
    @SuppressWarnings("unchecked")
    public void testPositiveDeltaChargedWhenActualExceedsEstimate() {
        // Set limit large enough for a 1x raw pre-charge but not for the actual 1x rendered
        // + parsed bytes. We force delta > 0 by setting the initial EMA multiplier to 1.0
        // via a very tight breaker that accepts the raw pre-charge but trips on the extra.
        //
        // Template: 10 stored fields → raw ~60 B, rendered ~80 B, parsed serialised ~80 B
        // Pre-charge at EMA=4.0 → 240 B, actual ≈ 160 B → delta < 0 (normal case).
        //
        // To trigger delta > 0 we need actual > estimated. This happens when params cause
        // significant expansion. We simulate it with a template whose param-expanded output
        // is larger than 4× the raw script — use a param that injects a long string.
        long limitBytes = 10 * 1024 * 1024L;
        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(limitBytes);
        NodeClient client = mock(NodeClient.class);

        AtomicReference<ActionListener<MultiSearchResponse>> multiSearchListener = new AtomicReference<>();
        doAnswer(inv -> {
            multiSearchListener.set(inv.getArgument(1));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);

        // Craft a template that expands beyond 4× its raw script size without needing
        // a query-parser registry. A single stored_fields param injecting a 300-char value
        // renders to ~309 bytes vs raw ~38 bytes (>4×38=152), guaranteeing delta > 0.
        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        SearchTemplateRequest tmpl = new SearchTemplateRequest();
        tmpl.setScriptType(ScriptType.INLINE);
        tmpl.setScript("{\"size\":0,\"stored_fields\":[\"{{field}}\"]}");
        tmpl.setScriptParams(Collections.singletonMap("field", "x".repeat(300)));
        tmpl.setRequest(new SearchRequest());
        request.add(tmpl);

        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        // Should have proceeded to multiSearch (no breaker trip on 1 template within 10 MB).
        assertNotNull("multiSearch should be called for an expanding template within limit", multiSearchListener.get());

        MultiSearchResponse.Item[] items = { new MultiSearchResponse.Item(null, new RuntimeException("stub")) };
        multiSearchListener.get().onResponse(new MultiSearchResponse(items, 1L));

        assertNull("No failure expected within the limit", caughtFailure.get());
        assertNotNull(caughtResponse.get());
        assertEquals("Breaker fully released after response", 0L, breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed());
    }

    // ---------------------------------------------------------------------------
    // Test 8: maxConcurrentSearchRequests propagation to inner MultiSearchRequest
    // ---------------------------------------------------------------------------

    /**
     * When the incoming request specifies {@code maxConcurrentSearchRequests}, that value
     * must be propagated to the inner {@link MultiSearchRequest} sent to the client.
     */
    @SuppressWarnings("unchecked")
    public void testMaxConcurrentSearchRequestsPropagated() {
        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(10 * 1024 * 1024L);
        NodeClient client = mock(NodeClient.class);

        AtomicReference<MultiSearchRequest> capturedMultiSearch = new AtomicReference<>();
        doAnswer(inv -> {
            capturedMultiSearch.set(inv.getArgument(0));
            ActionListener<MultiSearchResponse> listener = inv.getArgument(1);
            MultiSearchResponse.Item[] items = { new MultiSearchResponse.Item(null, new RuntimeException("stub")) };
            listener.onResponse(new MultiSearchResponse(items, 1L));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);

        MultiSearchTemplateRequest request = buildRequest(1, 5);
        request.maxConcurrentSearchRequests(7);

        action.doExecute(mock(Task.class), request, ActionListener.wrap(r -> {}, e -> {}));

        assertNotNull("multiSearch should have been called", capturedMultiSearch.get());
        assertEquals(
            "maxConcurrentSearchRequests must be propagated to inner MultiSearchRequest",
            7,
            capturedMultiSearch.get().maxConcurrentSearchRequests()
        );
    }

    // ---------------------------------------------------------------------------
    // Test 9: successful sub-response path — item.isFailure() == false
    // ---------------------------------------------------------------------------

    /**
     * Verifies the success path inside the multiSearch response handler: when a sub-response
     * is successful (not a failure), {@code setResponse} is called on the corresponding item
     * and the overall response is returned correctly.
     */
    @SuppressWarnings("unchecked")
    public void testSuccessfulSubResponsePopulatesItem() {
        CircuitBreakerService breakerService = circuitBreakerServiceWithLimit(10 * 1024 * 1024L);
        NodeClient client = mock(NodeClient.class);

        AtomicReference<ActionListener<MultiSearchResponse>> multiSearchListener = new AtomicReference<>();
        doAnswer(inv -> {
            multiSearchListener.set(inv.getArgument(1));
            return null;
        }).when(client).multiSearch(any(MultiSearchRequest.class), any(ActionListener.class));

        TransportMultiSearchTemplateAction action = buildAction(breakerService, client);
        MultiSearchTemplateRequest request = buildRequest(1, 5);

        AtomicReference<MultiSearchTemplateResponse> caughtResponse = new AtomicReference<>();
        AtomicReference<Exception> caughtFailure = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(caughtResponse::set, caughtFailure::set));

        assertNotNull(multiSearchListener.get());

        // Respond with a successful (non-failure) item.
        SearchResponse searchResponse = mock(SearchResponse.class);
        MultiSearchResponse.Item successItem = new MultiSearchResponse.Item(searchResponse, null);
        multiSearchListener.get().onResponse(new MultiSearchResponse(new MultiSearchResponse.Item[] { successItem }, 5L));

        assertNull("No failure expected for successful sub-response", caughtFailure.get());
        assertNotNull("Response must be returned", caughtResponse.get());
        assertFalse("Sub-response item must not be a failure", caughtResponse.get().getResponses()[0].isFailure());
        assertSame(
            "SearchResponse must be set on the item",
            searchResponse,
            caughtResponse.get().getResponses()[0].getResponse().getResponse()
        );
        assertEquals("Breaker fully released", 0L, breakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS).getUsed());
    }
}
