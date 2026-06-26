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
      * {@code block_cache.foyer.size=50%} → Foyer gets 400&nbsp;GB, FileCache gets 400&nbsp;GB.
     *
     * <p>Default: {@code 50%}. Set to {@code 0%} to disable the block cache.
     * Accepts a percentage (e.g. {@code 50%}) or a ratio (e.g. {@code 0.50}).
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.foyer.size: 50%
     * }</pre>
     */
    public static final Setting<String> CACHE_SIZE_SETTING = new Setting<>("block_cache.foyer.size", "50%", value -> {
        try {
            RatioValue ratio = RatioValue.parseRatioValue(value);
            if (ratio.getAsRatio() < 0 || ratio.getAsRatio() >= 1.0) {
                throw new IllegalArgumentException("[block_cache.foyer.size] must be in [0%, 100%); got: " + value);
            }
            return value;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "[block_cache.foyer.size] must be a percentage (e.g. 25%) or ratio (e.g. 0.25); got: " + value,
                e
            );
        }
    }, Setting.Property.NodeScope);

    /**
     * Block size for the Foyer disk tier.
     *
     * <p>Must be &ge; the largest entry ever put into the cache. Parquet row groups
     * can be up to 128&nbsp;MB. A block size smaller than an entry causes a silent
     * drop — the put succeeds but the entry is not stored, resulting in a cache miss.
     *
     * <p>Default: 128&nbsp;MB. Range: [1&nbsp;MB, 512&nbsp;MB].
     */
    public static final Setting<ByteSizeValue> BLOCK_SIZE_SETTING = Setting.byteSizeSetting(
        "block_cache.foyer.block_size",
        new ByteSizeValue(128, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(512, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * Total buffer pool size for the Foyer flusher.
     *
     * <p>The flusher stages entries in this buffer before writing to disk. Must be
     * &ge; {@code block_size} so the flusher can accumulate a full block. Entries
     * larger than this buffer are silently dropped.
     *
     * <p>Default: 128&nbsp;MB. Range: [16&nbsp;MB, 512&nbsp;MB].
     */
    public static final Setting<ByteSizeValue> BUFFER_POOL_SIZE_SETTING = Setting.byteSizeSetting(
        "block_cache.foyer.buffer_pool_size",
        new ByteSizeValue(128, ByteSizeUnit.MB),
        new ByteSizeValue(16, ByteSizeUnit.MB),
        new ByteSizeValue(512, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    /**
     * Submit queue size threshold for the Foyer block engine.
     *
     * <p>Maximum total bytes allowed to be pending in the flusher queue. When
     * exceeded, new entries are silently dropped instead of being written to disk.
     * Should be &ge; 2&times; {@code buffer_pool_size} to absorb write bursts.
     *
     * <p>Default: 256&nbsp;MB. Range: [16&nbsp;MB, 1024&nbsp;MB].
     */
    public static final Setting<ByteSizeValue> SUBMIT_QUEUE_SIZE_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "block_cache.foyer.submit_queue_size_threshold",
        new ByteSizeValue(256, ByteSizeUnit.MB),
        new ByteSizeValue(16, ByteSizeUnit.MB),
        new ByteSizeValue(1024, ByteSizeUnit.MB),
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
     * How often (seconds) the background sweeper prunes stale key_index entries left
     * by Foyer's disk reclaimer. {@code 0} = disabled (no background sweep task is spawned).
     * Range: [0, 3600]. Configure via {@code block_cache.foyer.key_index_sweep_interval_seconds}.
     */
    public static final Setting<Long> KEY_INDEX_SWEEP_INTERVAL_SETTING = Setting.longSetting(
        "block_cache.foyer.key_index_sweep_interval_seconds",
        0L,    // 0 = disabled (no sweep task spawned)
        0L,    // min: 0
        3600L, // max: 1 hour
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Minimum {@code used_bytes / disk_bytes} ratio required to run the key_index sweep.
     *
     * <p>On each interval tick the sweep loop checks whether the current usage ratio is
     * strictly below this threshold. If so, the sweep is skipped (no-op) — no DashMap
     * locks are acquired and no shard is iterated. This avoids wasting CPU cycles when
     * the cache is lightly loaded and Foyer's disk reclaimer is unlikely to have evicted
     * anything.
     *
     * <p>Default: {@code 0.70} — skip the sweep when the cache is less than 70% full.
     * Set to {@code 0.0} to disable the threshold guard and always sweep.
     *
     * <p>Range: {@code [0.0, 1.0]}.
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.foyer.key_index_sweep_threshold: 0.75
     * }</pre>
     */
    public static final Setting<Double> KEY_INDEX_SWEEP_THRESHOLD_SETTING = Setting.doubleSetting(
        "block_cache.foyer.key_index_sweep_threshold",
        0.70, // default: skip sweep when cache < 70% full
        0.0,  // min: 0.0 (explicit 0 = always sweep)
        1.0,  // max: 1.0
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often (seconds) the independent persist task flushes the key_index to disk.
     *
     * <p>The persist task is decoupled from the sweep task so that persist frequency
     * (a cheap file write, default 60 s) and sweep frequency (an expensive DashMap scan)
     * can be tuned independently.
     *
     * <p>The task uses {@code used_bytes} as a change signal: if {@code used_bytes} has
     * not changed since the last successful write, the tick is skipped (no disk I/O).
     * This means idle caches produce zero I/O even if the interval is short.
     *
     * <p>{@code 0} = disabled — the key_index is only persisted on graceful shutdown
     * via the {@code Drop} impl (i.e. when the JVM shuts down cleanly). In this mode
     * the maximum durability window after a crash equals the node uptime since startup.
     *
     * <p>Default: {@code 60} seconds. Range: [0, 3600].
     *
     * <p>Configure in {@code opensearch.yml}:
     * <pre>{@code
     * block_cache.foyer.key_index_persist_interval_seconds: 60
     * }</pre>
     */
    public static final Setting<Long> KEY_INDEX_PERSIST_INTERVAL_SETTING = Setting.longSetting(
        "block_cache.foyer.key_index_persist_interval_seconds",
        60L,   // default: 60 seconds
        0L,    // min: 0 (0 = disabled, persist only on graceful shutdown)
        3600L, // max: 1 hour
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Fraction of the total Foyer disk budget allocated to the metadata cache.
     *
     * <p>The remaining {@code (1 - ratio)} goes to the data cache. Applied against the
     * total Foyer disk bytes computed from {@code block_cache.foyer.size}.
     *
     * <p>Example: 400&nbsp;GB Foyer budget, {@code metadata_cache_ratio=5%} →
     * metadata cache gets 20&nbsp;GB, data cache gets 380&nbsp;GB.
     *
     * <p>Default: {@code 5%}. Set to {@code 0%} to disable tiered caching (single
     * data-only cache). Accepts a percentage (e.g. {@code 5%}) or a ratio (e.g. {@code 0.05}).
     *
     * <p>Range: [0%, 50%). Values &ge; 50% are rejected — metadata should never consume
     * more than half the SSD budget.
     */
    public static final Setting<String> METADATA_CACHE_RATIO_SETTING = new Setting<>(
        "block_cache.foyer.metadata_cache_ratio",
        "5%",
        value -> {
            try {
                RatioValue ratio = RatioValue.parseRatioValue(value);
                if (ratio.getAsRatio() < 0 || ratio.getAsRatio() >= 0.5) {
                    throw new IllegalArgumentException("[block_cache.foyer.metadata_cache_ratio] must be in [0%, 50%); got: " + value);
                }
                return value;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "[block_cache.foyer.metadata_cache_ratio] must be a percentage (e.g. 5%) or ratio (e.g. 0.05); got: " + value,
                    e
                );
            }
        },
        Setting.Property.NodeScope
    );

    /**
     * Block size for the metadata cache's Foyer disk tier.
     *
     * <p>Metadata entries (page indexes, offset indexes) are typically small (KB–MB range).
     * A smaller block size than the data cache avoids wasting SSD space on internal
     * fragmentation for these small entries.
     *
     * <p>Default: 8&nbsp;MB. Range: [1&nbsp;MB, 128&nbsp;MB].
     */
    public static final Setting<ByteSizeValue> METADATA_BLOCK_SIZE_SETTING = Setting.byteSizeSetting(
        "block_cache.foyer.metadata_block_size",
        new ByteSizeValue(8, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(128, ByteSizeUnit.MB),
        Setting.Property.NodeScope
    );

    private FoyerBlockCacheSettings() {}
}
