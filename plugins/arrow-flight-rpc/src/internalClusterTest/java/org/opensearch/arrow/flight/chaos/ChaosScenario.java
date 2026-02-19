/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.chaos;

import org.opensearch.transport.stream.StreamException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Methodical client-side chaos scenarios for Flight transport
 */
public class ChaosScenario {

    public enum ClientFailureScenario {
        CLIENT_NODE_DOWN,        // Client node shutdown after sending request and recovers
        RESPONSE_TIMEOUT,        // Response never received/timeout
        SERVER_DOWN_BEFORE,      // server node drop before call
        SERVER_DOWN_AFTER,       // server node drop after call
        NODE_DOWN_PERM           // Node permanently down
    }

    private static final AtomicBoolean enabled = new AtomicBoolean(false);
    private static volatile ClientFailureScenario activeScenario;
    private static volatile long timeoutDelayMs = 5000;

    public static void enableScenario(ClientFailureScenario scenario) {
        activeScenario = scenario;
        enabled.set(true);
    }

    public static void disable() {
        enabled.set(false);
        activeScenario = null;
    }

    /**
     * Client-side chaos injection at response processing
     */
    public static void injectChaos() throws StreamException {
        if (!enabled.get()) {
            return;
        }

        switch (activeScenario) {
            case CLIENT_NODE_DOWN:
                // simulateUnresponsiveClient();
                break;
            case RESPONSE_TIMEOUT:
                simulateLongRunningOperation();
                break;
            case SERVER_DOWN_BEFORE:
                // simulateResponseTimeout();
                break;
            case SERVER_DOWN_AFTER:
                // simulateResourceLeak();
                break;
            case NODE_DOWN_PERM:
                // simulateClientFailover();
                break;
        }
    }

    private static void simulateLongRunningOperation() throws StreamException {
        try {
            Thread.sleep(timeoutDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void setTimeoutDelay(long delayMs) {
        timeoutDelayMs = delayMs;
    }
}
