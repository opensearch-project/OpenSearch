/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fips;

/**
 * A functional interface for creating SSL-related objects with FIPS-aware configuration.
 * <p>
 * Automatically detects whether FIPS mode is enabled and provides the appropriate keystore type,
 * file extension, JCA provider, and JSSE provider for creating SSL contexts, key managers, or
 * trust managers.
 *
 * @param <T> the type of SSL object to create
 */
@FunctionalInterface
public interface FipsAwareSslProvider<T> {

    default T create() {
        var cfg = FipsConfig.detect();
        return create(cfg.keyStoreType(), cfg.fileExtension(), cfg.jcaProvider(), cfg.jsseProvider());
    }

    T create(String keyStoreType, String fileExtension, String jcaProvider, String jsseProvider);

}
