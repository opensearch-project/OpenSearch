/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.RatioValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Set;

/**
 * Foyer-specific settings for the node-level block cache.
 *
 * <p>These settings are consumed only by {@link BlockCacheFoyerPlugin} and are
 * registered via {@link BlockCacheFoyerPlugin#getSettings()}. They are not visible
 * to server-side components.
 *
 * <p>All settings are namespaced under {@code block_cache.foyer.*} to distinguish
 * them from settings that might be registered by other {@code BlockCacheProvider}
 * plugins (e.g. {@code block_cache.caffeine.*}).
 *
 * <p>{@code block_cache.foyer.size} is owned here because the Foyer plugin is
 * responsible for reporting its requested capacity to core via
 * {@link BlockCacheFoyerPlugin#requestedCapacityBytes(org.opensearch.common.settings.Settings, long)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FoyerBlockCacheSettings {

    /**
     * Fraction of the total warm-cache SSD budget allocated to the Foyer block cache.
     *
     * <p>The total budget is defined by {@code node.search.cache.size} (default: 80%
     * of SSD on warm nodes). This setting is applied against that budget, so the actual
     * byte allocation scales automatically with the instance's SSD capacity.
     *
     * <p>Example: 1&nbsp;TB SSD, {@code node.search.cache.size=80%} (800&nbsp;GB budget),
     * {@code block_cache.foyer.size=25%} → Foyer gets 200&nbsp;GB, FileCache gets 600&nbsp;GB.
     *
     * <p>Default: {@code 25%}. Set to {@code 0%} to disable the block cache.
     * Accepts a percentage (e.g. {@code 25%}) or a ratio (e.g. {@code 0.25}).
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.foyer.size: 25%
     * }</pre>
     */
    public static final Setting<String> CACHE_SIZE_SETTING = new Setting<>(
        "block_cache.foyer.size",
        "25%",
        value -> {
            try {
                RatioValue ratio = RatioValue.parseRatioValue(value);
                if (ratio.getAsRatio() < 0 || ratio.getAsRatio() >= 1.0) {
                    throw new IllegalArgumentException(
                        "[block_cache.foyer.size] must be in [0%, 100%); got: " + value);
                }
                return value;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "[block_cache.foyer.size] must be a percentage (e.g. 25%) or ratio (e.g. 0.25); got: " + value, e);
            }
        },
        Setting.Property.NodeScope
    );


    /**
     * Block size for the Foyer disk tier.
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
     * block_cache.foyer.block_size: 64mb
     * }</pre>
     */
    public static final Setting<ByteSizeValue> BLOCK_SIZE_SETTING = Setting.byteSizeSetting(
        "block_cache.foyer.block_size",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(256, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * I/O engine for the Foyer disk tier.
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
     * block_cache.foyer.io_engine: auto
     * }</pre>
     */
    public static final Setting<String> IO_ENGINE_SETTING = new Setting<>("block_cache.foyer.io_engine", "auto", value -> {
        if (!Set.of("auto", "io_uring", "psync").contains(value)) {
            throw new IllegalArgumentException("[block_cache.foyer.io_engine] must be one of: auto, io_uring, psync; got: " + value);
        }
        return value;
    }, Setting.Property.NodeScope);

    /**
     * Data-to-cache amplification ratio for the Foyer block cache.
     *
     * <p>For every byte of Foyer SSD capacity, the warm node can virtually serve this
     * many bytes of remote data. Used by warm-node capacity reporting
     * ({@code WarmFsService}) for shard placement decisions.
     *
     * <p>Default: {@code 5.0}.
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.foyer.data_to_cache_ratio: 5.0
     * }</pre>
     */
    public static final Setting<Double> DATA_TO_CACHE_RATIO_SETTING = Setting.doubleSetting(
        "block_cache.foyer.data_to_cache_ratio",
        5.0,
        1.0,
        Setting.Property.NodeScope
    );

    private FoyerBlockCacheSettings() {}
}
