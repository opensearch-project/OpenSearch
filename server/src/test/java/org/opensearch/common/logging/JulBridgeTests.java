/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.logging;

import org.apache.logging.log4j.jul.Log4jBridgeHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class JulBridgeTests extends OpenSearchTestCase {

    public void testLog4jBridgeHandlerInstallation() {
        Logger julRootLogger = LogManager.getLogManager().getLogger("");

        // Install the bridge (removes existing handlers including ConsoleHandler)
        Log4jBridgeHandler.install(true, null, true);

        Handler[] handlers = julRootLogger.getHandlers();
        boolean hasBridgeHandler = Arrays.stream(handlers).anyMatch(h -> h instanceof Log4jBridgeHandler);
        boolean hasConsoleHandler = Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler);

        assertTrue("Log4jBridgeHandler should be installed on JUL root logger", hasBridgeHandler);
        assertFalse("ConsoleHandler should be removed from JUL root logger after bridge installation", hasConsoleHandler);
    }

    public void testLog4jBridgeHandlerInstallationIsIdempotent() {
        Logger julRootLogger = LogManager.getLogManager().getLogger("");

        // Calling install twice should not result in duplicate handlers
        Log4jBridgeHandler.install(true, null, true);
        Log4jBridgeHandler.install(true, null, true);

        long bridgeHandlerCount = Arrays.stream(julRootLogger.getHandlers())
            .filter(h -> h instanceof Log4jBridgeHandler)
            .count();

        assertEquals("Log4jBridgeHandler should only be installed once even after multiple install calls", 1, bridgeHandlerCount);
    }
}
