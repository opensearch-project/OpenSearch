/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs.native_store;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Stateless factory that creates native (Rust) GCS ObjectStore instances via FFM.
 *
 * <p>Intersection settings passed to Rust:
 * <ul>
 *   <li>{@code bucket} — from repo settings</li>
 * </ul>
 *
 * <p>Credentials are NOT passed — Rust uses Application Default Credentials (ADC).
 *
 * @opensearch.experimental
 */
public class GcsNativeObjectStorePlugin extends Plugin implements NativeRemoteObjectStoreProvider {

    /** Repository type handled by this provider. */
    public static final String TYPE = "gcs";

    private static final String FFM_CREATE = "gcs_create_store";
    private static final String FFM_DESTROY = "gcs_destroy_store";

    private static final MethodHandle GCS_CREATE_STORE;
    private static final MethodHandle GCS_DESTROY_STORE;

    static {
        final SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        final Linker linker = Linker.nativeLinker();
        GCS_CREATE_STORE = linker.downcallHandle(
            lib.find(FFM_CREATE).orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG
            )
        );
        GCS_DESTROY_STORE = linker.downcallHandle(
            lib.find(FFM_DESTROY).orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    public GcsNativeObjectStorePlugin(final Settings settings) {}

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
            return call.invoke(GCS_CREATE_STORE, config.segment(), config.len(), credProviderPtr);
        }
    }

    @Override
    public long createNativeStoreFromMetadata(final RepositoryMetadata metadata, final Settings nodeSettings) {
        try {
            final String configJson = buildConfigJson(metadata, nodeSettings);
            return createNativeStore(configJson);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to build native store config for repo [" + metadata.name() + "]", e);
        }
    }

    @Override
    public void destroyNativeStore(final long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(GCS_DESTROY_STORE, ptr);
        }
    }

    /**
     * Build config JSON for the Rust GCS backend from repo metadata and node settings.
     */
    static String buildConfigJson(final RepositoryMetadata metadata, final Settings nodeSettings) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("bucket", metadata.settings().get("bucket", ""));
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
