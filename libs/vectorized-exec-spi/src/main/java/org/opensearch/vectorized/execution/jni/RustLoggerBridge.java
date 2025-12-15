/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.jni;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Bridge class that allows Rust code to log messages through Java's logging framework.
 * This class provides static methods that can be called from Rust via JNI to enable
 * unified logging across Java and native code.
 *
 * <p>The Rust code should call these methods using JNI with the fully qualified class name:
 * {@code org/opensearch/vectorized/execution/jni/RustLoggerBridge}
 *
 * <p>This class is designed to be used by both parquet-data-format and engine-datafusion
 * modules for consistent logging from their respective Rust JNI implementations.
 */
public class RustLoggerBridge {

    private static final Logger logger = LogManager.getLogger(RustLoggerBridge.class);

    /**
     * Log an info level message from Rust code.
     *
     * @param message the message to log
     */
    public static void logInfo(String message) {
        logger.info(message);
    }

    /**
     * Log a warning level message from Rust code.
     *
     * @param message the message to log
     */
    public static void logWarn(String message) {
        logger.warn(message);
    }

    /**
     * Log an error level message from Rust code.
     *
     * @param message the message to log
     */
    public static void logError(String message) {
        logger.error(message);
    }

    /**
     * Log a debug level message from Rust code.
     *
     * @param message the message to log
     */
    public static void logDebug(String message) {
        logger.debug(message);
    }

    /**
     * Log a trace level message from Rust code.
     *
     * @param message the message to log
     */
    public static void logTrace(String message) {
        logger.trace(message);
    }
}
