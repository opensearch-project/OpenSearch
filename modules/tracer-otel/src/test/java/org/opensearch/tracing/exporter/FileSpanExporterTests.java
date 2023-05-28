/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.exporter;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.internal.AttributesMap;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.opensearch.common.logging.Loggers;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Locale;

public class FileSpanExporterTests extends OpenSearchTestCase {
    private static MockAppender appender;
    private static Logger testLogger1 = LogManager.getLogger(FileSpanExporter.TRACING_LOG_PREFIX);

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(testLogger1, appender);
    }

    @AfterClass
    public static void cleanup() {
        Loggers.removeAppender(testLogger1, appender);
        appender.stop();
    }

    private final FileSpanExporter spanExporter = new FileSpanExporter();

    public void testOutputFormat() {
        String name = "TestSpan";
        String traceId = "6b44b6eadda4535388c406d3ba8894d4";
        String spanId = "96a0878e2febe8ba";
        String parentSpanId = "72a0878e2febg8fc";
        SpanData mockedSpanData = Mockito.mock(SpanData.class);
        Mockito.when(mockedSpanData.getName()).thenReturn(name);
        Mockito.when(mockedSpanData.getTraceId()).thenReturn(traceId);
        Mockito.when(mockedSpanData.getSpanId()).thenReturn(spanId);
        Mockito.when(mockedSpanData.getParentSpanId()).thenReturn(parentSpanId);
        Mockito.when(mockedSpanData.getKind()).thenReturn(SpanKind.INTERNAL);
        Mockito.when(mockedSpanData.getInstrumentationScopeInfo())
            .thenReturn(InstrumentationScopeInfo.create("instrumentation-library-name", "1.0.0", null));

        AttributesMap attributesMap = AttributesMap.create(128, 128);
        attributesMap.put(AttributeKey.stringKey("SpanId"), spanId);

        Mockito.when(mockedSpanData.getAttributes()).thenReturn(attributesMap);

        spanExporter.export(Arrays.asList(mockedSpanData));
        String logData = appender.getLastEventAndReset().getMessage().getFormattedMessage();
        assertEquals(getExpectedLogMessage(name, traceId, spanId, parentSpanId, attributesMap), logData);
        assertEquals(CompletableResultCode.ofSuccess(), spanExporter.flush());
        assertEquals(CompletableResultCode.ofSuccess(), spanExporter.shutdown());

        spanExporter.export(Arrays.asList(mockedSpanData));
        assertNull(appender.getLastEventAndReset());
    }

    private String getExpectedLogMessage(String name, String traceId, String spanId, String parentSpanId, AttributesMap attributesMap) {
        return String.format(
            Locale.ROOT,
            "'%s'\t%s\t%s\t%s\t%s\t%d\t%d\t[%s:%s:%s]\t%s",
            name,
            traceId,
            spanId,
            parentSpanId,
            "INTERNAL",
            0,
            0,
            "tracer",
            "instrumentation-library-name",
            "1.0.0",
            attributesMap
        );
    }

}
