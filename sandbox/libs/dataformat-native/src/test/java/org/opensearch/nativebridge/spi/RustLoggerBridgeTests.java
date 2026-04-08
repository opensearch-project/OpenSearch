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

public class RustLoggerBridgeTests extends OpenSearchTestCase {

    public void testLogDebug() throws Exception {
        Logger logger = LogManager.getLogger(RustLoggerBridge.class);
        Configurator.setLevel(logger, Level.DEBUG);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(logger)) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("debug", RustLoggerBridge.class.getName(), Level.DEBUG, "debug message")
            );
            RustLoggerBridge.log(RustLoggerBridge.LogLevel.DEBUG.ordinal(), "debug message");
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogInfo() throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("info", RustLoggerBridge.class.getName(), Level.INFO, "info message")
            );
            RustLoggerBridge.log(RustLoggerBridge.LogLevel.INFO.ordinal(), "info message");
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogError() throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation("error", RustLoggerBridge.class.getName(), Level.ERROR, "error message")
            );
            RustLoggerBridge.log(RustLoggerBridge.LogLevel.ERROR.ordinal(), "error message");
            appender.assertAllExpectationsMatched();
        }
    }

    public void testLogInvalidLevelIsIgnored() throws Exception {
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(RustLoggerBridge.class))) {
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("negative", RustLoggerBridge.class.getName(), Level.DEBUG, "bad")
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("negative-info", RustLoggerBridge.class.getName(), Level.INFO, "bad")
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("negative-error", RustLoggerBridge.class.getName(), Level.ERROR, "bad")
            );
            RustLoggerBridge.log(-1, "bad");
            RustLoggerBridge.log(99, "bad");
            appender.assertAllExpectationsMatched();
        }
    }
}
