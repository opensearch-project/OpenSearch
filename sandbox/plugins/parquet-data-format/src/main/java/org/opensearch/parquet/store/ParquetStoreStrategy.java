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
import org.opensearch.parquet.engine.ParquetDataFormat;

import java.util.Optional;

/**
 * Store strategy for the parquet data format.
 *
 * <p>Declares the parquet file-naming convention (files live under the
 * {@code "parquet/"} subdirectory) and provides a factory for the per-shard
 * native file registry that tracks parquet files for the Rust reader.
 *
 * <p>All other behaviour (per-shard construction, remote-metadata seeding,
 * directory routing, close ordering) is handled by the store layer. This
 * class is a pure declaration.
 */
public final class ParquetStoreStrategy implements StoreStrategy {

    private static final NativeFileRegistryFactory FACTORY = ParquetNativeFileRegistry::new;

    @Override
    public String name() {
        return ParquetDataFormat.PARQUET_DATA_FORMAT_NAME;
    }

    @Override
    public Optional<NativeFileRegistryFactory> nativeFileRegistry() {
        return Optional.of(FACTORY);
    }
}
