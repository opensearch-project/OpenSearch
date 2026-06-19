/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Excludes long-lived Netty/gRPC threads owned by {@code FlightStreamPlugin} from
 * the suite-scope thread-leak check.
 *
 * <p>The Flight RPC plugin starts several Netty event-loop groups
 * ({@code os-grpc-boss-ELG-N}, {@code os-grpc-worker-ELG-N}) and a
 * {@code flight-eventloop-N} dispatcher pool when it boots. These pools are
 * scoped to the plugin instance — not to a test method — and shut down only
 * when the cluster node hosting the plugin shuts down. Suite-scope test cluster
 * teardown does eventually close them, but lingering carrier threads can be
 * picked up as leaked under the
 * {@code com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering}
 * window, which is wider for the resilience suites that run cancel-during-stream
 * scenarios (those exercise the streaming transport heavily).
 *
 * <p>This is not a real leak: the threads belong to the plugin's lifecycle,
 * not to per-test work, and their drain is bounded by node shutdown — which
 * the test framework handles. Filtering them lets the suite focus on actual
 * test-induced leaks (orphaned drain threads, fragment-task threads, etc.).
 */
public final class FlightTransportThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        return name.startsWith("os-grpc-boss-ELG-")
            || name.startsWith("os-grpc-worker-ELG-")
            || name.startsWith("flight-eventloop-");
    }
}
