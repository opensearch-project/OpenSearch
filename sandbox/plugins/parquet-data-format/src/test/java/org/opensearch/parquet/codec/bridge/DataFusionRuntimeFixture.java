/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.bridge;

import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Starts the DataFusion runtime manager and a global runtime for tests that open a real doc-values
 * cursor, since the cursor requires both and deliberately has no private fallback.
 *
 * <p>Calls the two native symbols directly rather than going through the
 * analytics-backend-datafusion plugin's {@code NativeBridge}. That class installs FFM upcall stubs
 * in its static initializer and resolves classes the plugin declares {@code compileOnly} - they are
 * provided at runtime by its {@code extendedPlugins} parent - so loading it from this module's test
 * classpath fails. Both symbols live in the same shared library this bridge already uses.
 */
final class DataFusionRuntimeFixture {

    private static final MethodHandle INIT_RUNTIME_MANAGER;
    private static final MethodHandle CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle CLOSE_GLOBAL_RUNTIME;

    private static final long MEMORY_POOL_LIMIT = 64L * 1024 * 1024;
    private static final long SPILL_LIMIT = 32L * 1024 * 1024;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        INIT_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.JAVA_INT,     // cpu_threads
                ValueLayout.JAVA_DOUBLE,  // datanode_multiplier
                ValueLayout.JAVA_DOUBLE   // coordinator_multiplier
            )
        );
        CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_create_global_runtime").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,    // memory_pool_limit
                ValueLayout.JAVA_LONG,    // cache_manager_ptr
                ValueLayout.ADDRESS,      // spill_dir_ptr
                ValueLayout.JAVA_LONG,    // spill_dir_len
                ValueLayout.JAVA_LONG     // spill_limit
            )
        );
        CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
    }

    private DataFusionRuntimeFixture() {}

    /**
     * Starts the runtime manager, which backs the cursor's IO runtime. Idempotent in effect: the
     * native side replaces the manager, and no test shuts it down, because the manager is
     * process-wide and tearing it down would break every later test in the JVM.
     */
    static void initRuntimeManager(int cpuThreads) {
        try {
            INIT_RUNTIME_MANAGER.invokeExact(cpuThreads, 1.0d, 1.0d);
        } catch (Throwable t) {
            throw new AssertionError("could not start the DataFusion runtime manager", t);
        }
    }

    /**
     * Creates a global runtime, which is what publishes the file-metadata cache the cursor reads
     * footers through. Returns the pointer to pass to {@link #closeGlobalRuntime}.
     */
    static long createGlobalRuntime(Path spillDir) {
        try (Arena arena = Arena.ofConfined()) {
            byte[] bytes = spillDir.toString().getBytes(StandardCharsets.UTF_8);
            MemorySegment dir = arena.allocate(bytes.length);
            MemorySegment.copy(bytes, 0, dir, ValueLayout.JAVA_BYTE, 0, bytes.length);
            return (long) CREATE_GLOBAL_RUNTIME.invokeExact(MEMORY_POOL_LIMIT, 0L, dir, (long) bytes.length, SPILL_LIMIT);
        } catch (Throwable t) {
            throw new AssertionError("could not create a DataFusion global runtime", t);
        }
    }

    static void closeGlobalRuntime(long runtimePtr) {
        try {
            CLOSE_GLOBAL_RUNTIME.invokeExact(runtimePtr);
        } catch (Throwable t) {
            throw new AssertionError("could not close the DataFusion global runtime", t);
        }
    }
}
