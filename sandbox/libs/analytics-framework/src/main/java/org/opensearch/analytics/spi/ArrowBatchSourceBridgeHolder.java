/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Process-wide registration point for the installed {@link ArrowBatchSourceBridge}. */
public final class ArrowBatchSourceBridgeHolder {

    private static final AtomicReference<ArrowBatchSourceBridge> BRIDGE = new AtomicReference<>();

    private ArrowBatchSourceBridgeHolder() {}

    /** Installs or replaces the node's Arrow batch source bridge. */
    public static void install(ArrowBatchSourceBridge bridge) {
        BRIDGE.set(Objects.requireNonNull(bridge, "bridge"));
    }

    /** Removes {@code bridge} if it is still the installed instance. */
    public static void remove(ArrowBatchSourceBridge bridge) {
        BRIDGE.compareAndSet(bridge, null);
    }

    public static boolean isAvailable() {
        return BRIDGE.get() != null;
    }

    /** Returns the installed bridge. */
    public static ArrowBatchSourceBridge get() {
        ArrowBatchSourceBridge bridge = BRIDGE.get();
        if (bridge == null) {
            throw new IllegalStateException("No ArrowBatchSourceBridge is installed");
        }
        return bridge;
    }
}
