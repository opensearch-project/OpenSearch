/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs.native_store;

import org.opensearch.common.settings.Settings;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.plugins.Plugin;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Stateless factory that creates native (Rust) local filesystem ObjectStore instances.
 * Primarily used for testing with FS-based repositories.
 *
 * @opensearch.experimental
 */
public class FsNativeObjectStorePlugin extends Plugin implements NativeRemoteObjectStoreProvider {

    /** Repository type handled by this provider. */
    public static final String TYPE = "fs";

    private static final String FFM_CREATE = "fs_create_store";
    private static final String FFM_DESTROY = "fs_destroy_store";

    private static final MethodHandle FS_CREATE_STORE;
    private static final MethodHandle FS_DESTROY_STORE;

    static {
        final SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        final Linker linker = Linker.nativeLinker();
        FS_CREATE_STORE = linker.downcallHandle(
            lib.find(FFM_CREATE).orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG
            )
        );
        FS_DESTROY_STORE = linker.downcallHandle(
            lib.find(FFM_DESTROY).orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    public FsNativeObjectStorePlugin(final Settings settings) {}

    @Override
    public String repositoryType() {
        return TYPE;
    }

    @Override
    public long createNativeStore(final String configJson) {
        try (var call = new NativeCall()) {
            final NativeCall.Str config = call.str(configJson);
            return call.invoke(FS_CREATE_STORE, config.segment(), config.len());
        }
    }

    @Override
    public void destroyNativeStore(final long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(FS_DESTROY_STORE, ptr);
        }
    }
}
