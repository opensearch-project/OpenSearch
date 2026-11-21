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

import java.io.Closeable;
import java.io.IOException;

/**
 * Wraps an {@link AwsCredentialsProvider} so that {@code resolveCredentials()} runs inside a
 * {@link SocketAccess#doPrivileged} block. Needed for IRSA / InstanceProfile providers which
 * fetch credentials over the network.
 */
public final class PrivilegedCredentialsProvider implements AwsCredentialsProvider, Closeable {

    private final AwsCredentialsProvider delegate;

    /**
     * Creates a wrapper around the given delegate.
     *
     * @param delegate the credentials provider to wrap
     */
    public PrivilegedCredentialsProvider(AwsCredentialsProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        return SocketAccess.doPrivileged(delegate::resolveCredentials);
    }

    @Override
    public void close() throws IOException {
        if (delegate instanceof AutoCloseable) {
            SocketAccess.doPrivilegedIOException(() -> {
                try {
                    ((AutoCloseable) delegate).close();
                } catch (IOException | RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException(e);
                }
                return null;
            });
        }
    }
}
