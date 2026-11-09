/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class RustLoggerBridgeTests extends OpenSearchTestCase {

    private static MemorySegment toSegment(Arena arena, String s) {
        return arena.allocateFrom(ValueLayout.JAVA_BYTE, s.getBytes(StandardCharsets.UTF_8));
    }

    public void testLogDebug() throws Exception {
        Logger logger = LogManager.getLogger(RustLoggerBridge.class);
        Configurator.setLevel(logger, Level.DEBUG);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger); Arena arena = Arena.ofConfined()) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("debug", RustLoggerBridge.class.getName(), Level.DEBUG, "debug message")
            );
            RustLoggerBridge.log(0, toSegment(arena, "debug message"), "debug message".length());
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogInfo() throws Exception {
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class));
            Arena arena = Arena.ofConfined()
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("info", RustLoggerBridge.class.getName(), Level.INFO, "info message")
            );
            RustLoggerBridge.log(1, toSegment(arena, "info message"), "info message".length());
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogError() throws Exception {
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class));
            Arena arena = Arena.ofConfined()
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("error", RustLoggerBridge.class.getName(), Level.ERROR, "error message")
            );
            RustLoggerBridge.log(2, toSegment(arena, "error message"), "error message".length());
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogInvalidLevelIsIgnored() throws Exception {
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class));
            Arena arena = Arena.ofConfined()
        ) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("negative", RustLoggerBridge.class.getName(), Level.DEBUG, "bad")
            );
            RustLoggerBridge.log(-1, toSegment(arena, "bad"), 3);
            RustLoggerBridge.log(99, toSegment(arena, "bad"), 3);
            appender.assertAllExpectationsMatched();
        }
    }
}
