/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.opensearch.index.engine.dataformat.NativeFileRegistryFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;

import java.util.Optional;

/**
 * Store strategy for the parquet data format.
 *
 * <p>Uses the default {@code owns} / {@code remotePath} behaviour inherited
 * from {@link StoreStrategy} (files live under {@code "parquet/"} prefix, blobs
 * are laid out at {@code basePath + "parquet/" + blobKey}). The store layer
 * supplies the format name when it invokes those methods, so the strategy
 * itself does not carry the name.
 *
 * <p>Provides a factory for the per-shard native file registry that tracks
 * parquet files for the Rust reader.
 */
public final class ParquetStoreStrategy implements StoreStrategy {

    private static final NativeFileRegistryFactory FACTORY = ParquetNativeFileRegistry::new;

    @Override
    public Optional<NativeFileRegistryFactory> nativeFileRegistry() {
        return Optional.of(FACTORY);
    }
}
