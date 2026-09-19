/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Base class for tests that open a real Parquet doc-values cursor: starts the per-JVM DataFusion
 * runtime manager and global runtime the cursor requires, removing cross-test-class ordering
 * dependence.
 *
 * <p>Thread-leak detection is off because the Tokio runtime manager is a per-JVM singleton whose
 * threads outlive any one test class.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class DataFusionBackedTestCase extends OpenSearchTestCase {

    /** Arrow allocator for fixtures that export vectors to the native writer. */
    protected BufferAllocator allocator;

    private long globalRuntimePtr;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        // Safe to repeat: the native side replaces the manager under a write lock, so a second test
        // class simply installs a fresh one. Deliberately never shut down - the manager is process-wide
        // and tearing it down would break every later test in this JVM.
        DataFusionRuntimeFixture.initRuntimeManager(2);
        globalRuntimePtr = DataFusionRuntimeFixture.createGlobalRuntime(createTempDir("datafusion-spill"));
        assertNotEquals("global runtime must start before a cursor can be opened", 0L, globalRuntimePtr);
        allocator = new RootAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        // Guarded so a failure in setUp surfaces itself rather than an NPE from tear-down.
        if (allocator != null) {
            allocator.close();
        }
        if (globalRuntimePtr != 0L) {
            DataFusionRuntimeFixture.closeGlobalRuntime(globalRuntimePtr);
        }
        super.tearDown();
    }
}
