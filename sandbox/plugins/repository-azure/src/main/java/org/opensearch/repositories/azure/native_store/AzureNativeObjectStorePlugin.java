/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure.native_store;

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
 * Stateless factory that creates native (Rust) Azure ObjectStore instances via FFM.
 *
 * @opensearch.experimental
 */
public class AzureNativeObjectStorePlugin extends Plugin implements NativeRemoteObjectStoreProvider {

    /** Repository type handled by this provider. */
    public static final String TYPE = "azure";

    private static final String FFM_CREATE = "azure_create_store";
    private static final String FFM_DESTROY = "azure_destroy_store";

    private static final MethodHandle AZURE_CREATE_STORE;
    private static final MethodHandle AZURE_DESTROY_STORE;

    static {
        final SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        final Linker linker = Linker.nativeLinker();
        AZURE_CREATE_STORE = linker.downcallHandle(
            lib.find(FFM_CREATE).orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG
            )
        );
        AZURE_DESTROY_STORE = linker.downcallHandle(
            lib.find(FFM_DESTROY).orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    public AzureNativeObjectStorePlugin(final Settings settings) {}

    @Override
    public String repositoryType() {
        return TYPE;
    }

    @Override
    public long createNativeStore(final String configJson) {
        return createNativeStore(configJson, 0L);
    }

    @Override
    public long createNativeStore(final String configJson, final long credProviderPtr) {
        try (var call = new NativeCall()) {
            final NativeCall.Str config = call.str(configJson);
            return call.invoke(AZURE_CREATE_STORE, config.segment(), config.len(), credProviderPtr);
        }
    }

    @Override
    public void destroyNativeStore(final long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(AZURE_DESTROY_STORE, ptr);
        }
    }
}
