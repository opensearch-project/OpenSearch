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
 * This class provides a static method that can be called from Rust via JNI to enable
 * unified logging across Java and native code.
 *
 * <p>The Rust code calls this method using JNI with the fully qualified class name:
 * {@code org/opensearch/vectorized/execution/jni/RustLoggerBridge}
 *
 * <p>This class is used by all Rust-based plugins and modules
 * (parquet-data-format, engine-datafusion, etc.) for consistent logging.
 */
public class RustLoggerBridge {

    private static final Logger logger = LogManager.getLogger(RustLoggerBridge.class);

    /**
     * Log levels that can be used when logging from Rust code.
     * The ordinal values (0-2) are used by Rust to specify the log level.
     */
    public enum LogLevel {
        /** Debug level logging */
        DEBUG,
        /** Info level logging */
        INFO,
        /** Error level logging */
        ERROR
    }

    /**
     * Log a message at the specified level from Rust code.
     * This method is called by Rust via JNI.
     *
     * @param level the log level ordinal (0=DEBUG, 1=INFO, 2=ERROR)
     * @param message the message to log
     */
    public static void log(int level, String message) {
        LogLevel[] levels = LogLevel.values();
        if (level < 0 || level >= levels.length) {
            logger.info("[LEVEL_{}] {}", level, message);
            return;
        }

        switch (levels[level]) {
            case DEBUG:
                logger.debug(message);
                break;
            case INFO:
                logger.info(message);
                break;
            case ERROR:
                logger.error(message);
                break;
        }
    }
}
