/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Ignores Iceberg's internal static worker thread pools for thread-leak detection.
 * Iceberg's {@code ThreadPools} class lazily spawns long-lived daemon worker pools
 * (named {@code iceberg-worker-pool-*} and similar) for manifest processing. These
 * pools are intentionally static singletons and cannot be cleanly shut down between
 * tests, so leak detection would otherwise fail every test that touches the Iceberg
 * catalog.
 */
public final class IcebergThreadLeakFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread thread) {
        String name = thread.getName();
        return name != null && (name.startsWith("iceberg-") || name.startsWith("ForkJoinPool.commonPool"));
    }
}
