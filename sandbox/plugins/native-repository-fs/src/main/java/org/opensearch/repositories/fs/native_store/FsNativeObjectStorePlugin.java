/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.fs.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Stateless factory that creates native (Rust) local filesystem ObjectStore instances.
 * Primarily used for testing with FS-based repositories.
 *
 * <p>Intersection settings passed to Rust:
 * <ul>
 *   <li>{@code base_path} — from repo {@code location} setting</li>
 * </ul>
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
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        FS_DESTROY_STORE = linker.downcallHandle(
            lib.find(FFM_DESTROY).orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    /** No-arg constructor for ExtensiblePlugin SPI discovery via createExtension(). */
    public FsNativeObjectStorePlugin() {}

    FsNativeObjectStorePlugin(final Settings settings) {}

    @Override
    public String repositoryType() {
        return TYPE;
    }

    @Override
    public NativeStoreRepository createNativeStore(final RepositoryMetadata metadata, final Settings nodeSettings) {
        try {
            final String configJson = buildConfigJson(metadata);
            final long ptr = invokeCreateStore(configJson);
            if (ptr > 0) {
                return new NativeStoreRepository(new NativeStoreHandle(ptr, this::invokeDestroyStore));
            }
            return NativeStoreRepository.EMPTY;
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to build native store config for repo [" + metadata.name() + "]", e);
        }
    }

    /**
     * Invoke the Rust FS create store FFM function.
     */
    private long invokeCreateStore(final String configJson) {
        try (var call = new NativeCall()) {
            final NativeCall.Str config = call.str(configJson);
            return call.invoke(FS_CREATE_STORE, config.segment(), config.len());
        }
    }

    /**
     * Invoke the Rust FS destroy store FFM function.
     */
    private void invokeDestroyStore(final long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(FS_DESTROY_STORE, ptr);
        }
    }

    /**
     * Build config JSON for the Rust FS backend from repo metadata.
     */
    static String buildConfigJson(final RepositoryMetadata metadata) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("base_path", metadata.settings().get("location", ""));
            builder.endObject();
            return builder.toString();
        }
    }

    /**
     * Creates a local FS ObjectStore for testing. Returns the native pointer.
     *
     * @param rootPath the root directory for the local filesystem store
     * @return native ObjectStore pointer, or 0 on failure
     */
    public static long createTestStore(final String rootPath) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("base_path", rootPath);
            builder.endObject();
            final String configJson = builder.toString();
            final FsNativeObjectStorePlugin plugin = new FsNativeObjectStorePlugin();
            return plugin.invokeCreateStore(configJson);
        }
    }
}
