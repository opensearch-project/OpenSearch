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

    /** Log level constant for TRACE */
    public static final int LEVEL_TRACE = 0;
    /** Log level constant for DEBUG */
    public static final int LEVEL_DEBUG = 1;
    /** Log level constant for INFO */
    public static final int LEVEL_INFO = 2;
    /** Log level constant for WARN */
    public static final int LEVEL_WARN = 3;
    /** Log level constant for ERROR */
    public static final int LEVEL_ERROR = 4;

    /**
     * Log a message at the specified level from Rust code.
     * This method is called by Rust via JNI.
     *
     * @param level the log level (0=TRACE, 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR)
     * @param message the message to log
     */
    public static void log(int level, String message) {
        switch (level) {
            case LEVEL_TRACE:
                logger.trace(message);
                break;
            case LEVEL_DEBUG:
                logger.debug(message);
                break;
            case LEVEL_INFO:
                logger.info(message);
                break;
            case LEVEL_WARN:
                logger.warn(message);
                break;
            case LEVEL_ERROR:
                logger.error(message);
                break;
            default:
                logger.info("[LEVEL_{}] {}", level, message);
        }
    }
}
