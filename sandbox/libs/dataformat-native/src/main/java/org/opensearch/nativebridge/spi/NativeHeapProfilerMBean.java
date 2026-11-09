/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

/**
 * MBean interface for native heap profiling operations.
 * <p>
 * Exposed via JMX at {@code org.opensearch.native:type=HeapProfiler}.
 * The CLI tool {@code bin/opensearch-heap-prof} invokes these methods via JMX Attach API.
 */
public interface NativeHeapProfilerMBean {

    /** Activate jemalloc heap profiling. Allocations will be sampled from this point. */
    void activate();

    /** Deactivate jemalloc heap profiling. Zero overhead after this call. */
    void deactivate();

    /**
     * Dump a heap profile to the specified file path.
     * @param path absolute path for the dump file
     * @return the path where the dump was written
     */
    String dump(String path);

    /**
     * Reset profiling state and change sample interval.
     * WARNING: discards all accumulated profiling data.
     * @param lgSample log2 of sample interval in bytes (15=32KB, 17=128KB, 19=512KB)
     */
    void reset(int lgSample);

    /** Returns true if profiling is currently active. */
    boolean isActive();
}
