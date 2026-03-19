/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Bridge class that allows Rust code to log messages through Java's logging framework via JNI.
 *
 * <p>Rust calls this using the fully qualified class name:
 * {@code org/opensearch/nativebridge/spi/RustLoggerBridge}
 */
public class RustLoggerBridge {

    private static final Logger logger = LogManager.getLogger(RustLoggerBridge.class);

    public enum LogLevel {
        DEBUG,
        INFO,
        ERROR
    }

    /**
     * Called from Rust via JNI.
     * @param level log level ordinal (0=DEBUG, 1=INFO, 2=ERROR)
     * @param message the message to log
     */
    public static void log(int level, String message) {
        LogLevel[] levels = LogLevel.values();
        if (level < 0 || level >= levels.length) {
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

    private RustLoggerBridge() {}
}
