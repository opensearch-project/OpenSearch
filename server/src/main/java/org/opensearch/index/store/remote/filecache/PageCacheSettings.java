/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Set;

/**
 * Settings for the node-level format (page) cache backed by Foyer.
 *
 * <p>All settings are {@link Setting.Property#NodeScope}: they are applied once at
 * node startup when the cache is constructed, and require a node restart to take
 * effect. The cache cannot be reconfigured on a live node.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PageCacheSettings {

    /**
     * Block size for the format cache disk tier.
     *
     * <p>Must be &ge; the largest entry ever put into the cache. DataFusion reads
     * Parquet row groups of up to 64&nbsp;MB; Lucene blocks are also up to 64&nbsp;MB.
     * A block size smaller than an entry causes a silent drop — the put succeeds but
     * the entry is not stored, resulting in a cache miss on the next read.
     *
     * <p>Default: 64&nbsp;MB. Range: [1&nbsp;MB, 256&nbsp;MB].
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * format_cache.block_size: 64mb
     * }</pre>
     */
    public static final Setting<ByteSizeValue> BLOCK_SIZE_SETTING = Setting.byteSizeSetting(
        "format_cache.block_size",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(256, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * I/O engine for the format cache disk tier.
     *
     * <ul>
     *   <li>{@code auto} (default) — selects io_uring on Linux &ge;&nbsp;5.1,
     *       falls back to psync otherwise.</li>
     *   <li>{@code io_uring} — force io_uring regardless of kernel detection.
     *       Fails at startup if io_uring is unavailable (e.g. blocked by seccomp
     *       or AppArmor in locked-down container environments).</li>
     *   <li>{@code psync} — force synchronous pread/pwrite. Use when io_uring is
     *       restricted or when predictable syscall-level profiling is needed.</li>
     * </ul>
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * format_cache.io_engine: auto
     * }</pre>
     */
    public static final Setting<String> IO_ENGINE_SETTING = new Setting<>(
        "format_cache.io_engine",
        "auto",
        value -> {
            if (!Set.of("auto", "io_uring", "psync").contains(value)) {
                throw new IllegalArgumentException(
                    "[format_cache.io_engine] must be one of: auto, io_uring, psync; got: " + value
                );
            }
            return value;
        },
        Setting.Property.NodeScope
    );

    private PageCacheSettings() {}
}
