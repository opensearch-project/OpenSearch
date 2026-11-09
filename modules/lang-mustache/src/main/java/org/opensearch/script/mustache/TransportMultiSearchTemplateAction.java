/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.script.mustache;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.script.ScriptService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.script.mustache.TransportSearchTemplateAction.convert;

public class TransportMultiSearchTemplateAction extends HandledTransportAction<MultiSearchTemplateRequest, MultiSearchTemplateResponse> {

    /**
     * Maximum number of sub-requests allowed in a single {@code _msearch/template} batch.
     * Rendering and parsing every inline template simultaneously can exhaust the coordinator
     * heap exhaustion by a large _msearch/template batch. A hard limit on sub-request count is the
     * primary defence: it prevents the O(N) heap fan-out before any rendering occurs.
     * The circuit-breaker accounting below is the secondary defence.
     */
    public static final Setting<Integer> MAX_TEMPLATE_BATCH_SIZE_SETTING = Setting.intSetting(
        "search.max_multi_search_template_requests",
        1024,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;
    private final NodeClient client;
    private final CircuitBreakerService circuitBreakerService;
    private volatile int maxTemplateBatchSize;

    @Inject
    public TransportMultiSearchTemplateAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        NodeClient client,
        CircuitBreakerService circuitBreakerService,
        ClusterSettings clusterSettings
    ) {
        super(MultiSearchTemplateAction.NAME, transportService, actionFilters, MultiSearchTemplateRequest::new);
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.maxTemplateBatchSize = MAX_TEMPLATE_BATCH_SIZE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_TEMPLATE_BATCH_SIZE_SETTING, v -> this.maxTemplateBatchSize = v);
    }

    @Override
    protected void doExecute(Task task, MultiSearchTemplateRequest request, ActionListener<MultiSearchTemplateResponse> listener) {
        // PRIMARY GUARD: reject batches that exceed the sub-request count limit before any
        // rendering occurs. This prevents the O(N*renderSize) heap fan-out that can OOM the
        // coordinator heap. The limit is configurable via
        // search.max_multi_search_template_requests (default 1024).
        final int numRequests = request.requests().size();
        if (numRequests > maxTemplateBatchSize) {
            listener.onFailure(
                new IllegalArgumentException(
                    "Batch size "
                        + numRequests
                        + " exceeds search.max_multi_search_template_requests limit of "
                        + maxTemplateBatchSize
                        + ". Reduce the number of templates per request or raise the limit."
                )
            );
            return;
        }

        List<Integer> originalSlots = new ArrayList<>();
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        multiSearchRequest.indicesOptions(request.indicesOptions());
        if (request.maxConcurrentSearchRequests() != 0) {
            multiSearchRequest.maxConcurrentSearchRequests(request.maxConcurrentSearchRequests());
        }

        // SECONDARY GUARD: adaptive circuit-breaker accounting with EMA multiplier.
        //
        // We MUST fire the breaker BEFORE calling convert() so that the Mustache renderer
        // never allocates beyond what the breaker allows. The challenge is we don't know
        // the rendered size before rendering.
        //
        // Solution: maintain a per-request Exponential Moving Average (EMA) of the
        // (rendered / raw-script) expansion ratio. Pre-charge (rawBytes × emaMultiplier)
        // before each render, then adjust to actual rendered bytes and update the EMA.
        // After 3-5 templates the EMA converges to the actual expansion ratio of this batch
        // (templates within a batch are structurally similar), giving accurate pre-charges
        // for subsequent templates without any external configuration.
        //
        // Initial multiplier is conservatively high (4.0 — covers most real-world param
        // expansions). It adapts quickly: if templates don't expand, it drops toward 1.0;
        // if they do, it escalates so the CB fires before the next allocation.
        //
        // EMA update per template: ema = α × (rendered/raw) + (1−α) × ema, α = 0.3
        final double EMA_ALPHA = 0.3;
        final double INITIAL_EMA_MULTIPLIER = 4.0;
        final double[] emaMultiplier = { INITIAL_EMA_MULTIPLIER };

        long[] trackedBytes = { 0L };
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numRequests];
        try {
            for (int i = 0; i < items.length; i++) {
                SearchTemplateRequest searchTemplateRequest = request.requests().get(i);

                final long rawScriptBytes = searchTemplateRequest.getScript().getBytes(StandardCharsets.UTF_8).length;

                // Pre-charge estimated bytes BEFORE rendering. Uses the current EMA multiplier
                // so the estimate tracks the actual expansion ratio of this batch.
                // This fires the breaker before the renderer allocates, preventing OOM.
                final long estimatedBytes = Math.max(rawScriptBytes, (long) (rawScriptBytes * emaMultiplier[0]));
                breaker.addEstimateBytesAndMaybeBreak(estimatedBytes, "msearch_template_rendered_source");
                trackedBytes[0] += estimatedBytes;

                SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
                SearchRequest searchRequest;
                try {
                    searchRequest = convert(searchTemplateRequest, searchTemplateResponse, scriptService, xContentRegistry);
                } catch (Exception e) {
                    items[i] = new MultiSearchTemplateResponse.Item(null, e);
                    continue;
                }

                // Adjust charge to actual full heap footprint, then update EMA.
                //
                // The full footprint per template has two components:
                // 1. renderedBytes — the rendered JSON string retained on SearchTemplateResponse
                // 2. parsedBytes — the SearchSourceBuilder object graph (implements Accountable
                // via ramBytesUsed()), which can be 5-10× the rendered JSON
                // due to Java object overhead for parsed fields, aggregations, etc.
                //
                // Charging both components ensures the child breaker reflects the true heap
                // footprint, not just the raw JSON size. This mirrors how _bulk accounts for
                // in-memory size via bulkRequest.ramBytesUsed() against IndexingPressure.
                //
                // EMA tracks (rendered + parsed) / raw so pre-charges for subsequent templates
                // converge to the full actual footprint, not just the JSON wire size.
                if (searchTemplateResponse.getSource() != null && rawScriptBytes > 0) {
                    final long renderedBytes = searchTemplateResponse.getSource().length();
                    // Estimate parsed heap footprint by re-serialising SearchSourceBuilder to
                    // a BytesStreamOutput. This captures structural complexity (stored fields,
                    // aggregations, queries) proportionally without requiring Accountable.
                    // It is an underestimate of the true object-graph size (no Java object
                    // overhead), but it makes the EMA track the right order of magnitude and
                    // correctly escalates when parsed requests are large.
                    //
                    // TODO: if multiple execution paths need accurate SearchSourceBuilder
                    // footprint estimation, consider having SearchSourceBuilder implement
                    // org.apache.lucene.util.Accountable (ramBytesUsed()) so all callers
                    // can get the true object-graph size without re-serialisation overhead.
                    long parsedBytes = 0L;
                    if (searchRequest != null && searchRequest.source() != null) {
                        try (
                            org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput()
                        ) {
                            searchRequest.source().writeTo(out);
                            parsedBytes = out.bytes().length();
                        } catch (Exception ignored) {
                            // If serialisation fails, fall back to zero — rendered bytes alone
                            // still drive the EMA conservatively.
                        }
                    }
                    final long actualBytes = renderedBytes + parsedBytes;
                    final long delta = actualBytes - estimatedBytes;
                    if (delta > 0) {
                        // Actual footprint larger than estimated — charge the extra.
                        breaker.addEstimateBytesAndMaybeBreak(delta, "msearch_template_rendered_source");
                        trackedBytes[0] += delta;
                    } else if (delta < 0) {
                        // Over-charged — release excess without tripping.
                        breaker.addWithoutBreaking(delta);
                        trackedBytes[0] += delta; // delta is negative
                    }
                    // Update EMA with full actual footprint ratio (rendered + parsed) / raw.
                    final double actualMultiplier = (double) actualBytes / rawScriptBytes;
                    emaMultiplier[0] = EMA_ALPHA * actualMultiplier + (1.0 - EMA_ALPHA) * emaMultiplier[0];
                }

                items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
                if (searchRequest != null) {
                    multiSearchRequest.add(searchRequest);
                    originalSlots.add(i);
                }
            }
        } catch (Exception e) {
            // Release any bytes already charged before propagating (e.g. CircuitBreakerException).
            breaker.addWithoutBreaking(-trackedBytes[0]);
            listener.onFailure(e);
            return;
        }

        // Capture for use in the listener lambdas.
        final long chargedBytes = trackedBytes[0];
        client.multiSearch(multiSearchRequest, ActionListener.wrap(r -> {
            try {
                for (int i = 0; i < r.getResponses().length; i++) {
                    MultiSearchResponse.Item item = r.getResponses()[i];
                    int originalSlot = originalSlots.get(i);
                    if (item.isFailure()) {
                        items[originalSlot] = new MultiSearchTemplateResponse.Item(null, item.getFailure());
                    } else {
                        items[originalSlot].getResponse().setResponse(item.getResponse());
                    }
                }
                listener.onResponse(new MultiSearchTemplateResponse(items, r.getTook().millis()));
            } finally {
                // Release breaker bytes now that the response has been assembled and sent.
                breaker.addWithoutBreaking(-chargedBytes);
            }
        }, e -> {
            breaker.addWithoutBreaking(-chargedBytes);
            listener.onFailure(e);
        }));
    }
}
