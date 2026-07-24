/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionConcurrencyLimiterStats;
import org.opensearch.action.ActionConcurrencyLimiterStats.ActionLimiterSnapshot;
import org.opensearch.action.ActionRequest;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.concurrency.limit.BurstAwareLimit;
import org.opensearch.concurrency.limit.OpenSearchVegasLimit;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import com.netflix.concurrency.limits.limit.AbstractLimit;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limit.GradientLimit;
import com.netflix.concurrency.limits.limiter.AbstractPartitionedLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;

/**
 * Registry that manages one adaptive concurrency limiter per configured action alias.
 * <p>
 * Operators configure limiters at runtime via cluster settings using an alias as the
 * namespace key:
 * <pre>
 *   concurrency_limit.action.search.action_name = indices:data/read/search
 *   concurrency_limit.action.search.mode        = enforced
 *   concurrency_limit.action.search.algorithm   = vegas
 * </pre>
 * Any action can be limited — no code change is required to add a new action.
 * <p>
 * Supported modes: {@code disabled} (default, no-op), {@code monitor_only} (tracks
 * metrics but never rejects), {@code enforced} (actively rejects when limit reached).
 * <p>
 * Partitioning: when {@code partitions} is set, the total limit is divided across
 * named sub-pools. A {@link PartitionResolver} maps each request to a partition.
 */
public class ActionConcurrencyLimiterRegistry {

    private static final Logger LOG = LogManager.getLogger(ActionConcurrencyLimiterRegistry.class);

    static final String SETTING_PREFIX = "concurrency_limit.action.";

    // -------------------------------------------------------------------------
    // AffixSettings — one per tunable parameter
    // -------------------------------------------------------------------------

    /** Maps an alias to the transport action name it limits. */
    public static final Setting.AffixSetting<String> ACTION_NAME_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "action_name",
        k -> Setting.simpleString(k, "", Property.Dynamic, Property.NodeScope)
    );

    static final Set<String> VALID_MODES = Set.of("disabled", "enforced", "monitor_only");

    /** Limiter mode: {@code disabled}, {@code enforced}, or {@code monitor_only}. */
    public static final Setting.AffixSetting<String> MODE_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "mode",
        k -> new Setting<>(k, "disabled", v -> {
            if (!VALID_MODES.contains(v)) {
                throw new IllegalArgumentException("Unknown mode '" + v + "'. Must be one of: " + VALID_MODES);
            }
            return v;
        }, Property.Dynamic, Property.NodeScope)
    );

    /** Limit algorithm: {@code vegas}, {@code gradient2}, or {@code aimd}. */
    public static final Setting.AffixSetting<String> ALGORITHM_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "algorithm",
        k -> new Setting<>(k, "vegas", v -> {
            try {
                LimitAlgorithm.valueOf(v.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown algorithm '" + v + "'. Must be one of: vegas, gradient2, aimd");
            }
            return v.toLowerCase(Locale.ROOT);
        }, Property.Dynamic, Property.NodeScope)
    );

    static final Setting.AffixSetting<Settings> LIMIT_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "limit",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** Duration after (re)configuration during which the limiter does not reject requests. */
    public static final Setting.AffixSetting<TimeValue> WARMUP_DURATION_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "warmup_duration",
        k -> Setting.timeSetting(k, TimeValue.timeValueMinutes(5), TimeValue.timeValueMinutes(0), Property.Dynamic, Property.NodeScope)
    );

    static final Setting.AffixSetting<Settings> AIMD_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "aimd",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    static final Setting.AffixSetting<Settings> GRADIENT2_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "gradient2",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    static final Setting.AffixSetting<Settings> VEGAS_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "vegas",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    static final Setting.AffixSetting<Settings> BURST_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "burst",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    // -------------------------------------------------------------------------
    // Partition settings
    // -------------------------------------------------------------------------

    /** Valid {@code partition.resolver} values. Empty string means "no resolver". */
    static final Set<String> VALID_RESOLVERS = Set.of("", "byHeader", "fixed", "bySearchType");

    /** Ordered list of partition names, e.g. {@code ["premium", "standard", "default"]}. */
    public static final Setting.AffixSetting<List<String>> PARTITIONS_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "partitions",
        k -> Setting.listSetting(k, List.of(), Function.identity(), Property.Dynamic, Property.NodeScope)
    );

    /**
     * Group setting capturing all per-alias partition configuration under
     * {@code concurrency_limit.action.<alias>.partition.*}. This includes:
     * <ul>
     *   <li>{@code <name>.percent} — share of total limit (0.0–1.0)</li>
     *   <li>{@code <name>.delay_ms} — optional reject-delay for this partition</li>
     *   <li>{@code resolver} — resolver type ({@code byHeader}, {@code fixed})</li>
     *   <li>{@code resolver.fixed.partition} — target partition for {@code fixed} resolver</li>
     * </ul>
     */
    public static final Setting.AffixSetting<Settings> PARTITION_CONFIG_SETTING = Setting.affixKeySetting(
        SETTING_PREFIX,
        "partition",
        k -> Setting.groupSetting(k + ".", Property.Dynamic, Property.NodeScope)
    );

    /** All settings registered by this module. */
    public static final List<Setting<?>> ALL_SETTINGS = List.of(
        ACTION_NAME_SETTING,
        MODE_SETTING,
        ALGORITHM_SETTING,
        LIMIT_CONFIG_SETTING,
        WARMUP_DURATION_SETTING,
        AIMD_CONFIG_SETTING,
        GRADIENT2_CONFIG_SETTING,
        VEGAS_CONFIG_SETTING,
        BURST_CONFIG_SETTING,
        PARTITIONS_SETTING,
        PARTITION_CONFIG_SETTING
    );

    // -------------------------------------------------------------------------
    // Algorithm enum
    // -------------------------------------------------------------------------

    enum LimitAlgorithm {
        VEGAS {
            @Override
            Limit build(AliasConfig cfg) {
                return OpenSearchVegasLimit.newBuilder()
                    .initialLimit(cfg.initialLimit)
                    .maxConcurrency(cfg.maxLimit)
                    .upDriftFactor(cfg.vegasUpDriftFactor)
                    .increaseHysteresis(cfg.vegasIncreaseBarrier)
                    .decreaseHysteresis(cfg.vegasDecreaseBarrier)
                    .probeInflightThreshold(cfg.vegasBaselineResetLoadThreshold)
                    .build();
            }
        },
        GRADIENT2 {
            @Override
            Limit build(AliasConfig cfg) {
                return Gradient2Limit.newBuilder()
                    .initialLimit(cfg.initialLimit)
                    .maxConcurrency(cfg.maxLimit)
                    .rttTolerance(cfg.gradient2RttTolerance)
                    .build();
            }
        },
        AIMD {
            @Override
            Limit build(AliasConfig cfg) {
                return AIMDLimit.newBuilder()
                    .initialLimit(cfg.initialLimit)
                    .minLimit(1)
                    .maxLimit(cfg.maxLimit)
                    .backoffRatio(cfg.aimdBackoffRatio)
                    .build();
            }
        };

        abstract Limit build(AliasConfig cfg);
    }

    // -------------------------------------------------------------------------
    // Per-alias mutable config
    // -------------------------------------------------------------------------

    static final class AliasConfig {
        volatile String actionName = "";
        volatile String mode = "disabled";
        volatile String algorithm = "vegas";
        volatile int initialLimit = 20;
        volatile int maxLimit = 200;
        volatile long warmupMillis = TimeValue.timeValueMinutes(5).getMillis();
        volatile double aimdBackoffRatio = 0.9;
        volatile double gradient2RttTolerance = 1.5;
        volatile int vegasUpDriftFactor = 1;
        volatile int vegasIncreaseBarrier = 1;
        volatile int vegasDecreaseBarrier = 1;
        volatile double vegasBaselineResetLoadThreshold = 0.5;
        volatile int burstCapacity = 0;
        volatile int burstCloseAfter = 5;
        volatile int burstOpenAfter = 5;
        // Partition config
        volatile List<String> partitions = List.of();
        volatile Settings partitionConfig = Settings.EMPTY;
    }

    // -------------------------------------------------------------------------
    // Per-alias limiter state
    // -------------------------------------------------------------------------

    private static final Limiter.Listener NOOP_LISTENER = new Limiter.Listener() {
        @Override
        public void onSuccess() {}

        @Override
        public void onIgnore() {}

        @Override
        public void onDropped() {}
    };

    static final class LimiterState {
        final Limit limit;
        final Limiter<SearchRequestContext> limiter;
        final long warmupUntil;
        boolean warmedUp;

        LimiterState(Limit limit, Limiter<SearchRequestContext> limiter, long warmupMillis) {
            this.limit = limit;
            this.limiter = limiter;
            this.warmedUp = false;
            this.warmupUntil = System.currentTimeMillis() + warmupMillis;
        }

        boolean isWarmedUp() {
            warmedUp = warmedUp || warmupUntil <= System.currentTimeMillis();
            return warmedUp;
        }
    }

    /**
     * Concrete builder for partitioned limiters. Extends {@link AbstractPartitionedLimiter.Builder}
     * to expose {@code partition()}, {@code partitionResolver()}, and {@code partitionRejectDelay()}.
     */
    private static final class PartitionedBuilder<ContextT> extends AbstractPartitionedLimiter.Builder<
        PartitionedBuilder<ContextT>,
        ContextT> {
        @Override
        protected PartitionedBuilder<ContextT> self() {
            return this;
        }
    }

    final class ActionLimiterInstance {
        private final String alias;
        private final AtomicReference<LimiterState> state = new AtomicReference<>();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicLong totalRejected = new AtomicLong();
        private volatile String actionName;
        private volatile String mode;
        private volatile String algorithm;
        // True when this limiter is partitioned. Used to avoid allocating a
        // SearchRequestContext on the hot path when partitioning is not in use.
        private volatile boolean partitioned;

        ActionLimiterInstance(String alias) {
            this.alias = alias;
        }

        synchronized void reconfigure(AliasConfig cfg) {
            // Build the new state into a local FIRST. If construction throws (e.g. invalid
            // partition percentages), no instance field is mutated and the caller's try/catch
            // leaves this instance fully on its previous configuration.
            LimiterState newState = null;
            boolean newPartitioned = !cfg.partitions.isEmpty();
            if (!"disabled".equals(cfg.mode)) {
                LimitAlgorithm algo = LimitAlgorithm.valueOf(cfg.algorithm.toUpperCase(Locale.ROOT));
                Limit limit = algo.build(cfg);
                if (cfg.burstCapacity > 0) {
                    limit = new BurstAwareLimit((AbstractLimit) limit, cfg.burstCapacity, cfg.burstCloseAfter, cfg.burstOpenAfter);
                }
                Limiter<SearchRequestContext> limiter = buildLimiter(limit, cfg);
                newState = new LimiterState(limit, limiter, cfg.warmupMillis);
            }

            // All construction succeeded — now commit. Write state BEFORE mode so that if
            // another thread reads mode (volatile) and sees a non-disabled value, state is
            // guaranteed to be non-null.
            this.actionName = cfg.actionName;
            this.algorithm = cfg.algorithm;
            this.partitioned = newPartitioned;
            if (newState != null) {
                LimiterState prev = state.get();
                if (prev != null && prev.isWarmedUp()) {
                    newState.warmedUp = true;
                }
                state.set(newState);
            }
            this.mode = cfg.mode;
        }

        String getMode() {
            return mode;
        }

        String getActionName() {
            return actionName;
        }

        boolean isPartitioned() {
            return partitioned;
        }

        Optional<Limiter.Listener> tryAcquire(Task task, ActionRequest request) {
            LimiterState s = state.get();
            if (s == null) return Optional.empty();

            // Only allocate the context object when the limiter is partitioned and actually
            // needs it to resolve a partition. The non-partitioned SimpleLimiter ignores it.
            SearchRequestContext ctx = partitioned ? new SearchRequestContext(task, actionName, request) : null;
            Optional<Limiter.Listener> token = s.limiter.acquire(ctx);
            if (token.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    Limit rejLimit = s.limit;
                    if (rejLimit instanceof BurstAwareLimit) {
                        int baseLimit = ((BurstAwareLimit) rejLimit).getDelegate().getLimit();
                        int effectiveLimit = rejLimit.getLimit();
                        LOG.debug(
                            "Request rejected for alias [{}] action [{}]: limit={} [base={}, burst={}] inFlight={}",
                            alias,
                            actionName,
                            effectiveLimit,
                            baseLimit,
                            effectiveLimit - baseLimit,
                            inFlight.get()
                        );
                    } else {
                        LOG.debug(
                            "Request rejected for alias [{}] action [{}]: limit={} inFlight={}",
                            alias,
                            actionName,
                            rejLimit.getLimit(),
                            inFlight.get()
                        );
                    }
                }
                if ("monitor_only".equals(this.mode)) {
                    totalRejected.incrementAndGet();
                    return Optional.of(NOOP_LISTENER);
                }
                // enforced: reject after warmup, pass through during warmup
                if (!s.isWarmedUp()) return Optional.of(NOOP_LISTENER);
                totalRejected.incrementAndGet();
                return Optional.empty();
            }
            inFlight.incrementAndGet();
            Limiter.Listener inner = token.get();
            return Optional.of(new Limiter.Listener() {
                @Override
                public void onSuccess() {
                    inFlight.decrementAndGet();
                    inner.onSuccess();
                }

                @Override
                public void onIgnore() {
                    inFlight.decrementAndGet();
                    inner.onIgnore();
                }

                @Override
                public void onDropped() {
                    inFlight.decrementAndGet();
                    inner.onDropped();
                }
            });
        }

        ActionLimiterSnapshot snapshot() {
            LimiterState s = state.get();
            int currentLimit = s != null ? s.limit.getLimit() : 0;
            long lastRttMs = -1L;
            long rttNoLoadMs = -1L;
            if (s != null) {
                Limit limit = s.limit;
                if (limit instanceof BurstAwareLimit) {
                    limit = ((BurstAwareLimit) limit).getDelegate();
                }
                if (limit instanceof GradientLimit) {
                    lastRttMs = ((GradientLimit) limit).getLastRtt(TimeUnit.MILLISECONDS);
                    rttNoLoadMs = ((GradientLimit) limit).getRttNoLoad(TimeUnit.MILLISECONDS);
                } else if (limit instanceof Gradient2Limit) {
                    lastRttMs = ((Gradient2Limit) limit).getLastRtt(TimeUnit.MILLISECONDS);
                    rttNoLoadMs = ((Gradient2Limit) limit).getRttNoLoad(TimeUnit.MILLISECONDS);
                } else if (limit instanceof OpenSearchVegasLimit) {
                    rttNoLoadMs = ((OpenSearchVegasLimit) limit).getRttNoLoad(TimeUnit.MILLISECONDS);
                }
            }
            return new ActionLimiterSnapshot(
                alias,
                actionName,
                mode,
                algorithm,
                currentLimit,
                inFlight.get(),
                totalRejected.get(),
                lastRttMs,
                rttNoLoadMs
            );
        }
    }

    // -------------------------------------------------------------------------
    // Registry state
    // -------------------------------------------------------------------------

    private final ConcurrentHashMap<String, AliasConfig> aliasConfigs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ActionLimiterInstance> limiters = new ConcurrentHashMap<>();
    private volatile Map<String, ActionLimiterInstance> actionToInstance = Collections.emptyMap();
    private final ThreadContext threadContext;

    // -------------------------------------------------------------------------
    // Metrics listener (push telemetry)
    // -------------------------------------------------------------------------

    /**
     * Lifecycle callback for a push-telemetry bridge. The registry notifies the listener whenever an
     * alias becomes active (configured with a non-disabled mode and an action name) or inactive. The
     * {@code snapshotSupplier} yields the same per-alias values the pull path ({@code snapshot()} →
     * {@code NodeStats}) reports, so both telemetry paths publish identical numbers.
     */
    public interface MetricsListener {
        /** Called when an alias becomes active or is reconfigured.
         * @param alias the limiter alias
         * @param snapshotSupplier supplies the live snapshot for the alias */
        void onActive(String alias, Supplier<ActionLimiterSnapshot> snapshotSupplier);

        /** Called when an alias becomes inactive (disabled or removed).
         * @param alias the limiter alias */
        void onInactive(String alias);
    }

    private volatile MetricsListener metricsListener;
    private final Set<String> activeAliases = ConcurrentHashMap.newKeySet();

    /**
     * Attaches the push-telemetry listener and replays the currently-active aliases (those loaded
     * from initial settings before the listener was wired). Called once, after construction, from the
     * plugin's telemetry-aware {@code createComponents}.
     *
     * @param listener the metrics listener to attach, or {@code null} to detach
     */
    public synchronized void setMetricsListener(MetricsListener listener) {
        this.metricsListener = listener;
        if (listener == null) return;
        limiters.forEach((alias, inst) -> {
            if (isActive(inst)) {
                activeAliases.add(alias);
                listener.onActive(alias, inst::snapshot);
            }
        });
    }

    private static boolean isActive(ActionLimiterInstance inst) {
        return inst != null && !"disabled".equals(inst.getMode()) && !inst.getActionName().isEmpty();
    }

    /** Fire the metrics listener for one alias after a (re)configuration. Called under synchronization. */
    private void notifyMetricsListener(String alias) {
        MetricsListener listener = this.metricsListener;
        if (listener == null) return;
        ActionLimiterInstance inst = limiters.get(alias);
        if (isActive(inst)) {
            activeAliases.add(alias);
            // Re-fire on every reconfigure so the bridge can refresh tags (mode/algorithm/action);
            // the listener is expected to treat a repeat onActive as a refresh.
            listener.onActive(alias, inst::snapshot);
        } else if (activeAliases.remove(alias)) {
            listener.onInactive(alias);
        }
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a new registry, loading initial limiter configurations and registering dynamic update consumers.
     *
     * @param initialSettings the node's initial settings
     * @param clusterSettings the cluster settings service for dynamic updates
     * @param threadContext the thread context for header propagation
     */
    public ActionConcurrencyLimiterRegistry(Settings initialSettings, ClusterSettings clusterSettings, ThreadContext threadContext) {
        this.threadContext = threadContext;

        // Load all aliases present in initial settings
        Set<String> aliases = new HashSet<>();
        for (Setting<?> s : ALL_SETTINGS) {
            aliases.addAll(((Setting.AffixSetting<?>) s).getNamespaces(initialSettings));
        }
        for (String alias : aliases) {
            AliasConfig cfg = buildConfig(alias, initialSettings);
            aliasConfigs.put(alias, cfg);
            onAliasReconfigured(alias, cfg);
        }

        // Register update consumers
        clusterSettings.addAffixUpdateConsumer(ACTION_NAME_SETTING, (alias, val) -> {
            aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig()).actionName = val;
            onAliasReconfigured(alias, aliasConfigs.get(alias));
        }, (alias, val) -> {});
        clusterSettings.addAffixUpdateConsumer(MODE_SETTING, (alias, val) -> {
            aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig()).mode = val;
            onAliasReconfigured(alias, aliasConfigs.get(alias));
        }, (alias, val) -> {});
        clusterSettings.addAffixUpdateConsumer(ALGORITHM_SETTING, (alias, val) -> {
            aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig()).algorithm = val;
            onAliasReconfigured(alias, aliasConfigs.get(alias));
        }, (alias, val) -> {});
        clusterSettings.addAffixUpdateConsumer(LIMIT_CONFIG_SETTING, (alias, val) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.initialLimit = val.getAsInt("initial", 20);
            cfg.maxLimit = val.getAsInt("max", 200);
            onAliasReconfigured(alias, cfg);
        }, (alias, val) -> validateLimitConfig(val));
        clusterSettings.addAffixUpdateConsumer(WARMUP_DURATION_SETTING, (alias, val) -> {
            aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig()).warmupMillis = val.getMillis();
            onAliasReconfigured(alias, aliasConfigs.get(alias));
        }, (alias, val) -> {});
        clusterSettings.addAffixUpdateConsumer(AIMD_CONFIG_SETTING, (alias, val) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.aimdBackoffRatio = val.getAsDouble("backoff_ratio", 0.9);
            onAliasReconfigured(alias, cfg);
        }, (alias, val) -> validateAimdConfig(val));
        clusterSettings.addAffixUpdateConsumer(GRADIENT2_CONFIG_SETTING, (alias, val) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.gradient2RttTolerance = val.getAsDouble("rtt_tolerance", 1.5);
            onAliasReconfigured(alias, cfg);
        }, (alias, val) -> validateGradient2Config(val));
        clusterSettings.addAffixUpdateConsumer(VEGAS_CONFIG_SETTING, (alias, val) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.vegasUpDriftFactor = val.getAsInt("updrift_factor", 1);
            cfg.vegasIncreaseBarrier = val.getAsInt("increase_barrier", 1);
            cfg.vegasDecreaseBarrier = val.getAsInt("decrease_barrier", 1);
            cfg.vegasBaselineResetLoadThreshold = val.getAsDouble("baseline_reset_load_threshold", 0.5);
            onAliasReconfigured(alias, cfg);
        }, (alias, val) -> validateVegasConfig(val));
        clusterSettings.addAffixUpdateConsumer(BURST_CONFIG_SETTING, (alias, val) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.burstCapacity = val.getAsInt("capacity", 0);
            cfg.burstCloseAfter = val.getAsInt("close_after", 5);
            cfg.burstOpenAfter = val.getAsInt("open_after", 5);
            onAliasReconfigured(alias, cfg);
        }, (alias, val) -> validateBurstConfig(val));
        // partitions list and partition.* group are validated together (cross-field rules need
        // both), so they share one compound consumer. The validator runs on update only and
        // rejects a bad PUT with HTTP 400.
        clusterSettings.addAffixUpdateConsumer(PARTITIONS_SETTING, PARTITION_CONFIG_SETTING, (alias, tuple) -> {
            AliasConfig cfg = aliasConfigs.computeIfAbsent(alias, k -> new AliasConfig());
            cfg.partitions = tuple.v1();
            cfg.partitionConfig = tuple.v2();
            onAliasReconfigured(alias, cfg);
        }, (alias, tuple) -> validatePartitionConfig(tuple.v1(), tuple.v2()));
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Attempts to acquire a token for the given action. The {@code task} and {@code request}
     * are only used to build a {@link SearchRequestContext} for partition resolution when the
     * action's limiter is partitioned; for non-partitioned limiters no context is allocated.
     * Returns empty if no limiter is configured for the action or the limit is reached.
     *
     * @param action the transport action name
     * @param task the current task
     * @param request the action request
     */
    public Optional<Limiter.Listener> tryAcquire(String action, Task task, ActionRequest request) {
        ActionLimiterInstance inst = actionToInstance.get(action);
        if (inst == null) return Optional.empty();
        return inst.tryAcquire(task, request);
    }

    /**
     * Returns true if an active (non-disabled) limiter is configured for this action.
     *
     * @param action the transport action name
     */
    public boolean hasLimiterFor(String action) {
        return actionToInstance.containsKey(action);
    }

    /** Returns a snapshot of all configured limiter states. */
    public ActionConcurrencyLimiterStats getStats() {
        List<ActionLimiterSnapshot> snapshots = new ArrayList<>();
        limiters.values().forEach(inst -> snapshots.add(inst.snapshot()));
        return new ActionConcurrencyLimiterStats(snapshots);
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    static void validateLimitConfig(Settings group) {
        int initial = group.getAsInt("initial", 20);
        int max = group.getAsInt("max", 200);
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
        int upDrift = group.getAsInt("updrift_factor", 1);
        int incBarrier = group.getAsInt("increase_barrier", 1);
        int decBarrier = group.getAsInt("decrease_barrier", 1);
        double threshold = group.getAsDouble("baseline_reset_load_threshold", 0.5);
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
        int capacity = group.getAsInt("capacity", 0);
        int closeAfter = group.getAsInt("close_after", 5);
        int openAfter = group.getAsInt("open_after", 5);
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
        double backoff = group.getAsDouble("backoff_ratio", 0.9);
        if (backoff < 0.5 || backoff >= 1.0) {
            throw new IllegalArgumentException("aimd.backoff_ratio must be in [0.5, 1.0) but got " + backoff);
        }
    }

    static void validateGradient2Config(Settings group) {
        double rttTolerance = group.getAsDouble("rtt_tolerance", 1.5);
        if (rttTolerance < 1.0) {
            throw new IllegalArgumentException("gradient2.rtt_tolerance must be >= 1.0 but got " + rttTolerance);
        }
    }

    /**
     * Validates the partition configuration for one alias at update time so invalid config is
     * rejected with HTTP 400 rather than silently swallowed at limiter-build time. Wired into the
     * compound affix update consumer (update-only — does not run on {@code get()}), so an
     * already-persisted bad value cannot break startup reads.
     */
    static void validatePartitionConfig(List<String> partitions, Settings group) {
        // Per-partition percent range + cumulative sum; non-negative delay_ms.
        double sum = 0.0;
        for (String key : group.keySet()) {
            if (key.endsWith(".percent")) {
                double pct = group.getAsDouble(key, 0.0);
                if (pct < 0.0 || pct > 1.0) {
                    throw new IllegalArgumentException("partition percent [" + key + "] must be in [0.0, 1.0] but got " + pct);
                }
                sum += pct;
            } else if (key.endsWith(".delay_ms")) {
                if (group.getAsLong(key, 0L) < 0) {
                    throw new IllegalArgumentException("partition [" + key + "] must be >= 0");
                }
            }
        }
        if (sum > 1.0) {
            throw new IllegalArgumentException("Sum of partition percentages must be <= 1.0 but got " + sum);
        }

        String resolver = group.get("resolver", "");
        if (!VALID_RESOLVERS.contains(resolver)) {
            throw new IllegalArgumentException(
                "Unknown partition resolver [" + resolver + "]. Must be one of: byHeader, fixed, bySearchType"
            );
        }

        // Cross-field: partitions configured but no way to route requests to them. All traffic
        // would funnel to unknownPartition (hard limit 1) and collapse. Reject early.
        if (!partitions.isEmpty() && resolver.isEmpty()) {
            throw new IllegalArgumentException(
                "partitions "
                    + partitions
                    + " configured but no partition.resolver set; "
                    + "all requests would route to unknownPartition. Set partition.resolver "
                    + "(byHeader or fixed)."
            );
        }

        // Cross-field: the fixed resolver must target a partition that exists in the list.
        if ("fixed".equals(resolver)) {
            String target = group.getAsSettings("resolver.fixed").get("partition", "default");
            if (!partitions.contains(target)) {
                throw new IllegalArgumentException(
                    "partition.resolver.fixed.partition [" + target + "] is not in partitions " + partitions
                );
            }
        }
    }

    /**
     * Builds a {@link Limiter} for the given limit, wiring up partition resolver
     * and per-partition configuration when partitions are defined.
     * <p>
     * When no partitions are configured, uses {@link SimpleLimiter} directly (zero overhead).
     * When partitions are configured, uses {@link AbstractPartitionedLimiter} via
     * {@link PartitionedBuilder}, which divides the limit across named sub-pools.
     */
    private Limiter<SearchRequestContext> buildLimiter(Limit limit, AliasConfig cfg) {
        if (cfg.partitions.isEmpty()) {
            return SimpleLimiter.<SearchRequestContext>newBuilder().limit(limit).build();
        }

        Settings partConfig = cfg.partitionConfig;

        // Validate percentages up front so a bad value produces a clear error rather than
        // an opaque exception from deep inside the Netflix builder. Netflix enforces
        // each percent in [0.0, 1.0] and sum <= 1.0; mirror those checks here.
        double sum = 0.0;
        for (String partitionName : cfg.partitions) {
            double pct = partConfig.getAsDouble(partitionName + ".percent", 0.0);
            if (pct < 0.0 || pct > 1.0) {
                throw new IllegalArgumentException("partition [" + partitionName + "] percent must be in [0.0, 1.0] but got " + pct);
            }
            if (pct == 0.0) {
                LOG.warn(
                    "Partition [{}] for alias [{}] has 0%% capacity (no percent configured); "
                        + "all requests mapped to it will be rejected.",
                    partitionName,
                    cfg.actionName
                );
            }
            sum += pct;
        }
        if (sum > 1.0) {
            throw new IllegalArgumentException(
                "Sum of partition percentages for alias [" + cfg.actionName + "] must be <= 1.0 but got " + sum
            );
        }

        String resolverType = partConfig.get("resolver", "");
        // Defense-in-depth for the initial-load path, where the settings-layer compound validator
        // does not run: reject a missing/unknown resolver here so a persisted bad config is caught
        // (logged by onAliasReconfigured, previous limiter kept) instead of silently funneling all
        // traffic to unknownPartition.
        if (resolverType.isEmpty()) {
            throw new IllegalArgumentException("partitions configured for alias [" + cfg.actionName + "] but no partition.resolver set");
        }
        if (!VALID_RESOLVERS.contains(resolverType)) {
            throw new IllegalArgumentException("Unknown partition resolver [" + resolverType + "] for alias [" + cfg.actionName + "]");
        }
        Settings resolverConfig = partConfig.getAsSettings("resolver." + resolverType);
        PartitionResolver resolver = PartitionResolver.build(resolverType, resolverConfig, threadContext);

        PartitionedBuilder<SearchRequestContext> builder = new PartitionedBuilder<>();
        builder.limit(limit);
        builder.partitionResolver(ctx -> ctx != null ? resolver.resolve(ctx) : null);

        for (String partitionName : cfg.partitions) {
            double pct = partConfig.getAsDouble(partitionName + ".percent", 0.0);
            long delayMs = partConfig.getAsLong(partitionName + ".delay_ms", 0L);
            builder.partition(partitionName, pct);
            if (delayMs > 0) {
                builder.partitionRejectDelay(partitionName, delayMs, TimeUnit.MILLISECONDS);
            }
        }

        return builder.build();
    }

    private AliasConfig buildConfig(String alias, Settings s) {
        AliasConfig cfg = new AliasConfig();
        cfg.actionName = ACTION_NAME_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.mode = MODE_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.algorithm = ALGORITHM_SETTING.getConcreteSettingForNamespace(alias).get(s);
        Settings limitConfig = LIMIT_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.initialLimit = limitConfig.getAsInt("initial", 20);
        cfg.maxLimit = limitConfig.getAsInt("max", 200);
        cfg.warmupMillis = WARMUP_DURATION_SETTING.getConcreteSettingForNamespace(alias).get(s).getMillis();
        Settings aimdConfig = AIMD_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.aimdBackoffRatio = aimdConfig.getAsDouble("backoff_ratio", 0.9);
        Settings gradient2Config = GRADIENT2_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.gradient2RttTolerance = gradient2Config.getAsDouble("rtt_tolerance", 1.5);
        Settings vegasConfig = VEGAS_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.vegasUpDriftFactor = vegasConfig.getAsInt("updrift_factor", 1);
        cfg.vegasIncreaseBarrier = vegasConfig.getAsInt("increase_barrier", 1);
        cfg.vegasDecreaseBarrier = vegasConfig.getAsInt("decrease_barrier", 1);
        cfg.vegasBaselineResetLoadThreshold = vegasConfig.getAsDouble("baseline_reset_load_threshold", 0.5);
        Settings burstConfig = BURST_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.burstCapacity = burstConfig.getAsInt("capacity", 0);
        cfg.burstCloseAfter = burstConfig.getAsInt("close_after", 5);
        cfg.burstOpenAfter = burstConfig.getAsInt("open_after", 5);
        cfg.partitions = PARTITIONS_SETTING.getConcreteSettingForNamespace(alias).get(s);
        cfg.partitionConfig = PARTITION_CONFIG_SETTING.getConcreteSettingForNamespace(alias).get(s);
        return cfg;
    }

    private synchronized void onAliasReconfigured(String alias, AliasConfig cfg) {
        boolean reconfigured = true;
        if (cfg.actionName.isEmpty()) {
            limiters.remove(alias);
        } else {
            ActionLimiterInstance inst = limiters.computeIfAbsent(alias, ActionLimiterInstance::new);
            try {
                inst.reconfigure(cfg);
            } catch (RuntimeException e) {
                reconfigured = false;
                LOG.error(
                    "Failed to apply concurrency limit config for alias [{}] action [{}]; " + "keeping previous configuration. Error: {}",
                    alias,
                    cfg.actionName,
                    e.getMessage()
                );
            }
        }
        rebuildReverseMap();
        if (reconfigured) {
            notifyMetricsListener(alias);
        }
    }

    private void rebuildReverseMap() {
        Map<String, ActionLimiterInstance> newMap = new HashMap<>();
        limiters.forEach((alias, inst) -> {
            if (!"disabled".equals(inst.getMode()) && !inst.getActionName().isEmpty()) {
                newMap.put(inst.getActionName(), inst);
            }
        });
        actionToInstance = Collections.unmodifiableMap(newMap);
    }
}
