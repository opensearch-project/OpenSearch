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
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class JulBridgeTests extends OpenSearchTestCase {

    private Handler[] originalHandlers;
    private Logger julRootLogger;

    @Before
    public void saveJulHandlers() {
        julRootLogger = LogManager.getLogManager().getLogger("");
        originalHandlers = julRootLogger.getHandlers();
        // Remove all handlers to start from a known state
        for (Handler handler : originalHandlers) {
            julRootLogger.removeHandler(handler);
        }
        // Add a ConsoleHandler to simulate a pre-bridge state
        julRootLogger.addHandler(new ConsoleHandler());
    }

    @After
    public void restoreJulHandlers() {
        // Remove any handlers added during the test
        for (Handler handler : julRootLogger.getHandlers()) {
            julRootLogger.removeHandler(handler);
        }
        // Restore original handlers
        for (Handler handler : originalHandlers) {
            julRootLogger.addHandler(handler);
        }
    }

    public void testLog4jBridgeHandlerInstallation() {
        Log4jBridgeHandler.install(true, null, true);

        Handler[] handlers = julRootLogger.getHandlers();
        boolean hasBridgeHandler = Arrays.stream(handlers).anyMatch(h -> h instanceof Log4jBridgeHandler);
        boolean hasConsoleHandler = Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler);

        assertTrue("Log4jBridgeHandler should be installed on JUL root logger", hasBridgeHandler);
        assertFalse("ConsoleHandler should be removed from JUL root logger after bridge installation", hasConsoleHandler);
    }

    public void testLog4jBridgeHandlerInstallationIsIdempotent() {
        Log4jBridgeHandler.install(true, null, true);
        Log4jBridgeHandler.install(true, null, true);

        long bridgeHandlerCount = Arrays.stream(julRootLogger.getHandlers())
            .filter(h -> h instanceof Log4jBridgeHandler)
            .count();

        assertEquals("Log4jBridgeHandler should only be present once even after multiple install calls", 1, bridgeHandlerCount);
    }
}
