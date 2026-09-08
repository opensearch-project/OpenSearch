/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Setting definitions and validation for the concurrency limiter module.
 */
public final class ConcurrencyLimitSettings {

    private ConcurrencyLimitSettings() {}

    /** Common prefix for all concurrency limiter cluster settings. */
    public static final String SETTING_PREFIX = "concurrency_limit.action.";

    private static final class Defaults {
        static final String ACTION_NAME = "";
        static final String MODE = ConcurrencyLimitMode.DISABLED.getName();
        static final String ALGORITHM = "vegas";
        static final int INITIAL_LIMIT = 20;
        static final int MAX_LIMIT = 200;
        static final TimeValue WARMUP_DURATION = TimeValue.timeValueMinutes(5);
        static final double AIMD_BACKOFF_RATIO = 0.9;
        static final double GRADIENT2_RTT_TOLERANCE = 1.5;
        static final int VEGAS_UPDRIFT_FACTOR = 1;
        static final int VEGAS_INCREASE_BARRIER = 1;
        static final int VEGAS_DECREASE_BARRIER = 1;
        static final double VEGAS_BASELINE_RESET_LOAD_THRESHOLD = 0.5;
        static final int BURST_CAPACITY = 0;
        static final int BURST_CLOSE_AFTER = 5;
        static final int BURST_OPEN_AFTER = 5;
    }

    /** Maps an alias to the transport action name it limits. */
    public static final Setting.AffixSetting<String> ACTION_NAME = Setting.affixKeySetting(
        SETTING_PREFIX,
        "action_name",
        k -> Setting.simpleString(k, Defaults.ACTION_NAME, Property.Dynamic, Property.NodeScope)
    );

    /** Limiter mode. */
    public static final Setting.AffixSetting<String> MODE = Setting.affixKeySetting(
        SETTING_PREFIX,
        "mode",
        k -> new Setting<>(k, Defaults.MODE, v -> {
            ConcurrencyLimitMode.fromName(v);
            return v;
        }, Property.Dynamic, Property.NodeScope)
    );

    /** Limit algorithm: {@code vegas}, {@code gradient2}, or {@code aimd}. */
    public static final Setting.AffixSetting<String> ALGORITHM = Setting.affixKeySetting(
        SETTING_PREFIX,
        "algorithm",
        k -> new Setting<>(k, Defaults.ALGORITHM, v -> {
            try {
                ActionConcurrencyLimiterRegistry.LimitAlgorithm.valueOf(v.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown algorithm '" + v + "'. Must be one of: vegas, gradient2, aimd");
            }
            return v.toLowerCase(Locale.ROOT);
        }, Property.Dynamic, Property.NodeScope)
    );

    /** Group setting for limit bounds: {@code initial} and {@code max}. */
    public static final Setting.AffixSetting<Settings> LIMIT_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "limit",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Duration after (re)configuration during which the limiter does not reject requests. */
    public static final Setting.AffixSetting<TimeValue> WARMUP_DURATION = Setting.affixKeySetting(
        SETTING_PREFIX,
        "warmup_duration",
        k -> Setting.timeSetting(k, Defaults.WARMUP_DURATION, TimeValue.timeValueMinutes(0), Property.Dynamic, Property.NodeScope)
    );

    /** AIMD algorithm configuration. */
    public static final Setting.AffixSetting<Settings> AIMD_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "aimd",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Gradient2 algorithm configuration. */
    public static final Setting.AffixSetting<Settings> GRADIENT2_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "gradient2",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Vegas algorithm configuration. */
    public static final Setting.AffixSetting<Settings> VEGAS_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "vegas",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Burst configuration. */
    public static final Setting.AffixSetting<Settings> BURST_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "burst",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Ordered list of partition names. */
    public static final Setting.AffixSetting<List<String>> PARTITIONS = Setting.affixKeySetting(
        SETTING_PREFIX,
        "partitions",
        k -> Setting.listSetting(k, List.of(), Function.identity(), Property.Dynamic, Property.NodeScope)
    );

    /** Group setting for partition configuration. */
    public static final Setting.AffixSetting<Settings> PARTITION_CONFIG = Setting.affixKeySetting(
        SETTING_PREFIX,
        "partition",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** All settings registered by this module. */
    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        ACTION_NAME,
        MODE,
        ALGORITHM,
        LIMIT_CONFIG,
        WARMUP_DURATION,
        AIMD_CONFIG,
        GRADIENT2_CONFIG,
        VEGAS_CONFIG,
        BURST_CONFIG,
        PARTITIONS,
        PARTITION_CONFIG
    );

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    static void validateLimitConfig(Settings group) {
        int initial = group.getAsInt("initial", Defaults.INITIAL_LIMIT);
        int max = group.getAsInt("max", Defaults.MAX_LIMIT);
        if (initial < 1) {
            throw new IllegalArgumentException("limit.initial must be >= 1 but got " + initial);
        }
        if (max < 1) {
            throw new IllegalArgumentException("limit.max must be >= 1 but got " + max);
        }
        if (max < initial) {
            throw new IllegalArgumentException("limit.max [" + max + "] must be >= limit.initial [" + initial + "]");
        }
    }

    static void validateVegasConfig(Settings group) {
        int upDrift = group.getAsInt("updrift_factor", Defaults.VEGAS_UPDRIFT_FACTOR);
        int incBarrier = group.getAsInt("increase_barrier", Defaults.VEGAS_INCREASE_BARRIER);
        int decBarrier = group.getAsInt("decrease_barrier", Defaults.VEGAS_DECREASE_BARRIER);
        double threshold = group.getAsDouble("baseline_reset_load_threshold", Defaults.VEGAS_BASELINE_RESET_LOAD_THRESHOLD);
        if (upDrift < 1) {
            throw new IllegalArgumentException("vegas.updrift_factor must be >= 1 but got " + upDrift);
        }
        if (incBarrier < 1) {
            throw new IllegalArgumentException("vegas.increase_barrier must be >= 1 but got " + incBarrier);
        }
        if (decBarrier < 1) {
            throw new IllegalArgumentException("vegas.decrease_barrier must be >= 1 but got " + decBarrier);
        }
        if (threshold < 0.0 || threshold > 1.0) {
            throw new IllegalArgumentException("vegas.baseline_reset_load_threshold must be in [0.0, 1.0] but got " + threshold);
        }
    }

    static void validateBurstConfig(Settings group) {
        int capacity = group.getAsInt("capacity", Defaults.BURST_CAPACITY);
        int closeAfter = group.getAsInt("close_after", Defaults.BURST_CLOSE_AFTER);
        int openAfter = group.getAsInt("open_after", Defaults.BURST_OPEN_AFTER);
        if (capacity < 0) {
            throw new IllegalArgumentException("burst.capacity must be >= 0 but got " + capacity);
        }
        if (closeAfter < 1) {
            throw new IllegalArgumentException("burst.close_after must be >= 1 but got " + closeAfter);
        }
        if (openAfter < 1) {
            throw new IllegalArgumentException("burst.open_after must be >= 1 but got " + openAfter);
        }
    }

    static void validateAimdConfig(Settings group) {
        double backoff = group.getAsDouble("backoff_ratio", Defaults.AIMD_BACKOFF_RATIO);
        if (backoff < 0.5 || backoff >= 1.0) {
            throw new IllegalArgumentException("aimd.backoff_ratio must be in [0.5, 1.0) but got " + backoff);
        }
    }

    static void validateGradient2Config(Settings group) {
        double rttTolerance = group.getAsDouble("rtt_tolerance", Defaults.GRADIENT2_RTT_TOLERANCE);
        if (rttTolerance < 1.0) {
            throw new IllegalArgumentException("gradient2.rtt_tolerance must be >= 1.0 but got " + rttTolerance);
        }
    }

    static void validatePartitionConfig(List<String> partitions, Settings group) {
        double sum = 0.0;
        for (String partitionName : partitions) {
            double pct = group.getAsDouble(partitionName + ".percent", 0.0);
            if (pct < 0.0 || pct > 1.0) {
                throw new IllegalArgumentException(
                    "partition percent [" + partitionName + ".percent] must be in [0.0, 1.0] but got " + pct
                );
            }
            sum += pct;
            long delay = group.getAsLong(partitionName + ".delay_ms", 0L);
            if (delay < 0) {
                throw new IllegalArgumentException("partition [" + partitionName + ".delay_ms] must be >= 0");
            }
        }
        if (sum > 1.0 + 1e-6) {
            throw new IllegalArgumentException("Sum of partition percentages must be <= 1.0 but got " + sum);
        }

        String resolver = group.get("resolver", "");
        ConcurrencyLimitResolverType resolverType;
        try {
            resolverType = ConcurrencyLimitResolverType.fromName(resolver);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Unknown partition resolver [" + resolver + "]. Must be one of: byHeader, fixed, bySearchType"
            );
        }

        if (!partitions.isEmpty() && resolverType == ConcurrencyLimitResolverType.NONE) {
            throw new IllegalArgumentException(
                "partitions "
                    + partitions
                    + " configured but no partition.resolver set; "
                    + "all requests would route to unknownPartition. Set partition.resolver "
                    + "(byHeader or fixed)."
            );
        }

        if (resolverType == ConcurrencyLimitResolverType.FIXED) {
            String target = group.getAsSettings("resolver.fixed").get("partition", "default");
            if (!partitions.contains(target)) {
                throw new IllegalArgumentException(
                    "partition.resolver.fixed.partition [" + target + "] is not in partitions " + partitions
                );
            }
        }
    }
}
