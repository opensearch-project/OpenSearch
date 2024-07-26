/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.InternalThreadContextWrapper;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.telemetry.Telemetry;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;
import org.junit.After;
import org.junit.Before;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.opensearch.telemetry.TelemetrySettings.TRACER_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING;
import static org.opensearch.telemetry.TelemetrySettings.TRACER_SAMPLER_PROBABILITY;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class ThreadContextBasedTracerContextStorageTests extends OpenSearchTestCase {
    private Tracer tracer;
    private ThreadContext threadContext;
    private TracerContextStorage<String, Span> threadContextStorage;
    private ExecutorService executorService;

    @SuppressWarnings("resource")
    @Before
    public void setUp() throws Exception {
        super.setUp();

        final Settings settings = Settings.builder()
            .put(TRACER_ENABLED_SETTING.getKey(), true)
            .put(TRACER_SAMPLER_PROBABILITY.getKey(), 1d)
            .put(TRACER_FEATURE_ENABLED_SETTING.getKey(), true)
            .build();

        final TelemetrySettings telemetrySettings = new TelemetrySettings(
            settings,
            new ClusterSettings(Settings.EMPTY, Set.of(TRACER_ENABLED_SETTING, TRACER_SAMPLER_PROBABILITY))
        );

        final TracingTelemetry tracingTelemetry = new MockTracingTelemetry();

        threadContext = new ThreadContext(Settings.EMPTY);
        threadContextStorage = new ThreadContextBasedTracerContextStorage(threadContext, tracingTelemetry);

        tracer = new TracerFactory(telemetrySettings, Optional.of(new Telemetry() {
            @Override
            public MetricsTelemetry getMetricsTelemetry() {
                return null;
            }

            @Override
            public TracingTelemetry getTracingTelemetry() {
                return tracingTelemetry;
            }
        }), threadContext) {
            @Override
            protected TracerContextStorage<String, Span> createTracerContextStorage(
                TracingTelemetry tracingTelemetry,
                ThreadContext threadContext
            ) {
                return threadContextStorage;
            }
        }.getTracer();

        executorService = Executors.newSingleThreadExecutor();
        assertThat(tracer, not(instanceOf(NoopTracer.class)));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        executorService.shutdown();
        tracer.close();
    }

    public void testStartingSpanDoesNotChangeThreadContext() {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));
        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testSpanInScopeChangesThreadContext() {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        try (SpanScope scope = tracer.withSpanInScope(span)) {
            assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
            assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(span));
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testStashingPropagatesThreadContext() {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        try (SpanScope scope = tracer.withSpanInScope(span)) {
            try (StoredContext ignored = threadContext.stashContext()) {
                assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
                assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(span));
            }
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testPreservingContextThreadContext() throws InterruptedException, ExecutionException, TimeoutException {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        try (SpanScope scope = tracer.withSpanInScope(span)) {
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
                    assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(span));
                }
            };

            executorService.submit(threadContext.preserveContext(r)).get(1, TimeUnit.SECONDS);
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testNoThreadContextToPreserve() throws InterruptedException, ExecutionException, TimeoutException {
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
                assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));

                final Span local1 = tracer.startSpan(SpanCreationContext.internal().name("test-local-1"));
                try (SpanScope localScope = tracer.withSpanInScope(local1)) {
                    try (StoredContext ignored = threadContext.stashContext()) {
                        assertThat(local1.getParentSpan(), is(nullValue()));
                        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local1));
                    }
                }

                final Span local2 = tracer.startSpan(SpanCreationContext.internal().name("test-local-2"));
                try (SpanScope localScope = tracer.withSpanInScope(local2)) {
                    try (StoredContext ignored = threadContext.stashContext()) {
                        assertThat(local2.getParentSpan(), is(nullValue()));
                        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local2));
                    }
                }

                final Span local3 = tracer.startSpan(SpanCreationContext.internal().name("test-local-3"));
                try (SpanScope localScope = tracer.withSpanInScope(local3)) {
                    try (StoredContext ignored = threadContext.stashContext()) {
                        assertThat(local3.getParentSpan(), is(nullValue()));
                        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local3));
                    }
                }
            }
        };

        executorService.submit(threadContext.preserveContext(r)).get(1, TimeUnit.SECONDS);

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testPreservingContextThreadContextMultipleSpans() throws InterruptedException, ExecutionException, TimeoutException {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        try (SpanScope scope = tracer.withSpanInScope(span)) {
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
                    assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(span));

                    final Span local1 = tracer.startSpan(SpanCreationContext.internal().name("test-local-1"));
                    try (SpanScope localScope = tracer.withSpanInScope(local1)) {
                        try (StoredContext ignored = threadContext.stashContext()) {
                            assertThat(local1.getParentSpan(), is(span));
                            assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local1));
                        }
                    }

                    final Span local2 = tracer.startSpan(SpanCreationContext.internal().name("test-local-2"));
                    try (SpanScope localScope = tracer.withSpanInScope(local2)) {
                        try (StoredContext ignored = threadContext.stashContext()) {
                            assertThat(local2.getParentSpan(), is(span));
                            assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local2));
                        }
                    }

                    final Span local3 = tracer.startSpan(SpanCreationContext.internal().name("test-local-3"));
                    try (SpanScope localScope = tracer.withSpanInScope(local3)) {
                        try (StoredContext ignored = threadContext.stashContext()) {
                            assertThat(local3.getParentSpan(), is(span));
                            assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local3));
                        }
                    }
                }
            };

            executorService.submit(threadContext.preserveContext(r)).get(1, TimeUnit.SECONDS);
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testPreservingContextAndStashingThreadContext() throws InterruptedException, ExecutionException, TimeoutException {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        try (SpanScope scope = tracer.withSpanInScope(span)) {
            final Runnable r = new Runnable() {
                @Override
                public void run() {
                    final Span local = tracer.startSpan(SpanCreationContext.internal().name("test-local"));
                    try (SpanScope localScope = tracer.withSpanInScope(local)) {
                        try (StoredContext ignored = threadContext.stashContext()) {
                            assertThat(
                                threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN),
                                is(not(nullValue()))
                            );
                            assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(local));
                        }
                    }
                }
            };

            executorService.submit(threadContext.preserveContext(r)).get(1, TimeUnit.SECONDS);
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }

    public void testSpanNotPropagatedToChildSystemThreadContext() {
        final Span span = tracer.startSpan(SpanCreationContext.internal().name("test"));

        final InternalThreadContextWrapper tcWrapper = InternalThreadContextWrapper.from(threadContext);
        try (SpanScope scope = tracer.withSpanInScope(span)) {
            try (StoredContext ignored = threadContext.stashContext()) {
                assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
                assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(span));
                tcWrapper.markAsSystemContext();
                assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
            }
        }

        assertThat(threadContext.getTransient(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(not(nullValue())));
        assertThat(threadContextStorage.get(ThreadContextBasedTracerContextStorage.CURRENT_SPAN), is(nullValue()));
    }
}
