/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg.credentials;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Bridge between OpenSearch's credentials resolution and Iceberg's
 * {@code client.credentials-provider} reflection hook.
 * <p>
 * Iceberg loads credentials providers via {@code DynConstructors} — it reflects on the
 * class name and calls a static {@code create(Map<String,String>)} method (see
 * {@code org.apache.iceberg.aws.AwsClientProperties#credentialsProvider}). We can't hand
 * Iceberg a pre-built provider instance, but we can hand it a {@link Map} with the
 * registry key under which a {@link PrivilegedCredentialsProvider} instance has been
 * stashed. The static {@link #create} method pulls it back out.
 * <p>
 * The same hook is consumed by both {@code S3FileIO} (data plane) and the Iceberg REST
 * catalog's {@code RESTSigV4Signer} (control plane), so registering once covers both.
 */
public final class IcebergClientCredentialsProvider implements AwsCredentialsProvider {

    /** Property key under which {@link #register} stores and {@link #create} reads the UUID. */
    public static final String REGISTRY_KEY_PROPERTY = "client.credentials-provider.registry-key";

    private static final ConcurrentMap<String, AwsCredentialsProvider> REGISTRY = new ConcurrentHashMap<>();

    private final AwsCredentialsProvider delegate;

    private IcebergClientCredentialsProvider(AwsCredentialsProvider delegate) {
        this.delegate = delegate;
    }

    /**
     * Reflection entry point. Iceberg calls this with the properties map it was handed
     * via the {@code client.credentials-provider.*} prefix. Required property:
     * {@link #REGISTRY_KEY_PROPERTY}.
     *
     * @param properties properties passed from Iceberg's catalog configuration
     * @return a non-null credentials provider registered by the plugin
     */
    public static AwsCredentialsProvider create(Map<String, String> properties) {
        String key = properties.get(REGISTRY_KEY_PROPERTY);
        if (key == null) {
            throw new IllegalStateException(
                "expected [" + REGISTRY_KEY_PROPERTY + "] to be set on the Iceberg catalog properties but it was missing"
            );
        }
        AwsCredentialsProvider provider = REGISTRY.get(key);
        if (provider == null) {
            throw new IllegalStateException("no credentials provider registered for key [" + key + "]");
        }
        return new IcebergClientCredentialsProvider(provider);
    }

    /**
     * Registers the given provider and returns a fresh UUID key. The caller stores the
     * key on the Iceberg catalog properties under {@link #REGISTRY_KEY_PROPERTY}, then
     * {@link #deregister} when the catalog is closed.
     */
    public static String register(AwsCredentialsProvider provider) {
        String key = UUID.randomUUID().toString();
        REGISTRY.put(key, provider);
        return key;
    }

    /** Removes the given key from the registry. Idempotent. */
    public static void deregister(String key) {
        if (key != null) {
            REGISTRY.remove(key);
        }
    }

    /** Visible for tests: current registry size. */
    static int registrySize() {
        return REGISTRY.size();
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return delegate.resolveCredentials();
    }
}
