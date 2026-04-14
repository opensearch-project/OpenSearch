/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.native_store;

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
 * Stateless factory that creates native (Rust) S3 ObjectStore instances via FFM.
 *
 * <p>Resolves S3 configuration from repository metadata and node settings
 * (including keystore entries) and passes it to the Rust S3 backend.
 *
 * <p>Intersection settings passed to Rust:
 * <ul>
 *   <li>{@code bucket} — from repo settings</li>
 *   <li>{@code region} — from node {@code s3.client.{name}.region}</li>
 *   <li>{@code endpoint} — from node {@code s3.client.{name}.endpoint}</li>
 *   <li>{@code virtual_hosted_style} — inverted from node {@code s3.client.{name}.path_style_access}</li>
 *   <li>{@code allow_http} — from node {@code s3.client.{name}.protocol} == "http"</li>
 *   <li>{@code proxy_url} — from node {@code s3.client.{name}.proxy.host} + {@code proxy.port}</li>
 *   <li>{@code sse_kms_key_id} — from repo {@code server_side_encryption_kms_key_id}</li>
 *   <li>{@code bucket_key} — from repo {@code server_side_encryption_bucket_key_enabled}</li>
 *   <li>{@code max_retries} — from node {@code s3.client.{name}.max_retries}</li>
 * </ul>
 *
 * <p>Credentials are NOT passed — Rust uses the default credential chain (IAM roles, env vars).
 *
 * @opensearch.experimental
 */
public class S3NativeObjectStorePlugin extends Plugin implements NativeRemoteObjectStoreProvider {

    /** Repository type handled by this provider. */
    public static final String TYPE = "s3";

    /** Prefix for S3 client settings in node configuration. */
    static final String S3_CLIENT_PREFIX = "s3.client.";

    private static final String FFM_CREATE = "s3_create_store";
    private static final String FFM_DESTROY = "s3_destroy_store";

    private static final MethodHandle S3_CREATE_STORE;
    private static final MethodHandle S3_DESTROY_STORE;

    static {
        final SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        final Linker linker = Linker.nativeLinker();
        S3_CREATE_STORE = linker.downcallHandle(
            lib.find(FFM_CREATE).orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG
            )
        );
        S3_DESTROY_STORE = linker.downcallHandle(
            lib.find(FFM_DESTROY).orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    public S3NativeObjectStorePlugin(final Settings settings) {}

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
            return call.invoke(S3_CREATE_STORE, config.segment(), config.len(), credProviderPtr);
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
            call.invoke(S3_DESTROY_STORE, ptr);
        }
    }

    /**
     * Build config JSON for the Rust S3 backend from repo metadata and node settings.
     * Only includes intersection fields that both Java and Rust sides understand.
     */
    static String buildConfigJson(final RepositoryMetadata metadata, final Settings nodeSettings) throws IOException {
        final Settings repoSettings = metadata.settings();
        final String clientName = repoSettings.get("client", "default");
        final String cp = S3_CLIENT_PREFIX + clientName + ".";

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();

            builder.field("bucket", repoSettings.get("bucket", ""));

            final String region = nodeSettings.get(cp + "region", "");
            if (region.isEmpty() == false) {
                builder.field("region", region);
            }

            final String endpoint = nodeSettings.get(cp + "endpoint", "");
            if (endpoint.isEmpty() == false) {
                builder.field("endpoint", endpoint);
            }

            final boolean pathStyleAccess = nodeSettings.getAsBoolean(cp + "path_style_access", false);
            builder.field("virtual_hosted_style", !pathStyleAccess);

            final String protocol = nodeSettings.get(cp + "protocol", "https");
            if ("http".equalsIgnoreCase(protocol)) {
                builder.field("allow_http", true);
            }

            final String proxyHost = nodeSettings.get(cp + "proxy.host", "");
            if (proxyHost.isEmpty() == false) {
                final int proxyPort = nodeSettings.getAsInt(cp + "proxy.port", 80);
                final String scheme = "http".equalsIgnoreCase(protocol) ? "http" : "https";
                builder.field("proxy_url", scheme + "://" + proxyHost + ":" + proxyPort);
            }

            final String kmsKey = repoSettings.get("server_side_encryption_kms_key_id", "");
            if (kmsKey.isEmpty() == false) {
                builder.field("sse_kms_key_id", kmsKey);
            }

            builder.field("bucket_key", repoSettings.getAsBoolean("server_side_encryption_bucket_key_enabled", true));

            final int maxRetries = nodeSettings.getAsInt(cp + "max_retries", -1);
            if (maxRetries >= 0) {
                builder.field("max_retries", maxRetries);
            }

            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
