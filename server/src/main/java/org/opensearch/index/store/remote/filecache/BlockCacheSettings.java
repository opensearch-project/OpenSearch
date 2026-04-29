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
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Set;

/**
 * Settings for the node-level block cache backed by Foyer.
 *
 * <p>All settings are {@link Setting.Property#NodeScope}: they are applied once at
 * node startup when the cache is constructed, and require a node restart to take
 * effect. The cache cannot be reconfigured on a live node.
 *
 * <p>"Block" here is used in the storage sense — a contiguous, variable-size byte
 * range read as an indivisible I/O unit — not a fixed-size disk sector.
 * Entry granularity is determined by the calling layer (Parquet column chunks,
 * Lucene segment files) and may range from kilobytes to tens of megabytes.
 *
 * <h2>Capacity model</h2>
 *
 * <p>The warm-node SSD budget is split between FileCache (Lucene blocks) and the block
 * cache (variable-size byte ranges). Use {@code block_cache.size} to set what fraction of
 * the total {@code node.search.cache.size} budget goes to the block cache.
 *
 * <p>Example: budget = 80% of 1&nbsp;TB (800&nbsp;GB), {@code block_cache.size=25%}
 * → block cache gets 200&nbsp;GB, FileCache gets 600&nbsp;GB.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class BlockCacheSettings {

    /**
     * Block size for the block cache disk tier.
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
     * block_cache.block_size: 64mb
     * }</pre>
     */
    public static final Setting<ByteSizeValue> BLOCK_SIZE_SETTING = Setting.byteSizeSetting(
        "block_cache.block_size",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(256, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * I/O engine for the block cache disk tier.
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
     * block_cache.io_engine: auto
     * }</pre>
     */
    public static final Setting<String> IO_ENGINE_SETTING = new Setting<>("block_cache.io_engine", "auto", value -> {
        if (!Set.of("auto", "io_uring", "psync").contains(value)) {
            throw new IllegalArgumentException("[block_cache.io_engine] must be one of: auto, io_uring, psync; got: " + value);
        }
        return value;
    }, Setting.Property.NodeScope);

    /**
     * Fraction of the total warm-cache SSD budget allocated to the block cache.
     *
     * <p>This is the <em>preferred</em> way to size the block cache. The total budget is
     * defined by {@code node.search.cache.size} (default: 80% of SSD on warm nodes).
     * {@code block_cache.size} is applied against that budget, so the actual byte allocation
     * scales automatically with the instance's SSD capacity.
     *
     * <p>Example: on a 1&nbsp;TB SSD with {@code node.search.cache.size=80%} (800&nbsp;GB budget)
     * and {@code block_cache.size=25%}, the block cache receives 200&nbsp;GB and FileCache
     * receives 600&nbsp;GB.
     *
     * <p>Default: {@code 25%}.
     * Set to {@code 0%} to disable the block cache entirely.
     * Accepts a percentage (e.g. {@code 25%}) or a ratio (e.g. {@code 0.25}).
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.size: 25%
     * }</pre>
     */
    public static final Setting<String> CACHE_SIZE_SETTING = new Setting<>(
        "block_cache.size",
        "25%",
        value -> {
            try {
                RatioValue ratio = RatioValue.parseRatioValue(value);
                if (ratio.getAsRatio() < 0 || ratio.getAsRatio() >= 1.0) {
                    throw new IllegalArgumentException(
                        "[block_cache.size] must be in [0%, 100%); got: " + value);
                }
                return value;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "[block_cache.size] must be a percentage (e.g. 25%) or ratio (e.g. 0.25); got: " + value, e);
            }
        },
        Setting.Property.NodeScope
    );

    /**
     * Data-to-cache ratio for the block cache tier.
     * For every byte of block-cache SSD, the node can virtually serve this many bytes of remote data.
     * Default: 5.0.
     */
    public static final Setting<Double> DATA_TO_BLOCK_CACHE_SIZE_RATIO_SETTING = Setting.doubleSetting(
        "block_cache.data_to_cache_ratio",
        5.0,
        1.0,
        Setting.Property.NodeScope
    );

    private BlockCacheSettings() {}
}
