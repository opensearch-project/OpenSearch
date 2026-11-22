/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class TransportActionPrefixTests extends OpenSearchTestCase {
    public void testCustomPrefixesAreNodeLocalAndDelimited() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try (
            TransportService service = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool,
                NoopTracer.INSTANCE
            );
            TransportService other = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, threadPool, NoopTracer.INSTANCE)
        ) {
            service.registerAdditionalActionPrefixes(Set.of("report_instance:"));
            service.registerAdditionalActionPrefixes(Set.of("report_definition:"));
            expectThrows(IllegalArgumentException.class, () -> service.registerAdditionalActionPrefixes(Set.of("")));
            expectThrows(IllegalArgumentException.class, () -> service.registerAdditionalActionPrefixes(Set.of("report")));
            try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportService.class))) {
                appender.addExpectation(
                    new MockLogAppender.UnseenEventExpectation(
                        "registered namespace",
                        TransportService.class.getName(),
                        Level.WARN,
                        "*invalid action name*"
                    )
                );
                service.validateActionName("report_instance:get");
                service.validateActionName("report_definition:get");
                appender.assertAllExpectationsMatched();
            }
            try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(TransportService.class))) {
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "similar namespace",
                        TransportService.class.getName(),
                        Level.WARN,
                        "*invalid action name [report_instance_extra:get]*"
                    )
                );
                appender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "other service",
                        TransportService.class.getName(),
                        Level.WARN,
                        "*invalid action name [report_instance:get]*"
                    )
                );
                service.validateActionName("report_instance_extra:get");
                other.validateActionName("report_instance:get");
                appender.assertAllExpectationsMatched();
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }
}
