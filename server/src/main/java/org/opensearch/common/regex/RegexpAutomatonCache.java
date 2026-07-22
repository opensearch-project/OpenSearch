/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.regex;

import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongBiFunction;

/**
 * A process-wide LRU cache for compiled {@link CompiledAutomaton} objects used in regex queries.
 *
 * Keyed by (pattern, syntaxFlags, matchFlags, maxDeterminizedStates).
 * RAM-bounded based on {@link CompiledAutomaton#ramBytesUsed()}.
 * Dynamic cluster settings allow enabling/disabling and warm resizing of cache capacity.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public class RegexpAutomatonCache implements Closeable {

    public static final Setting<Boolean> REGEXP_AUTOMATON_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "search.regexp.automaton_cache.enabled",
        false,
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<ByteSizeValue> REGEXP_AUTOMATON_CACHE_MAX_SIZE_SETTING = Setting.memorySizeSetting(
        "search.regexp.automaton_cache.max_size_bytes",
        "50mb",
        Property.NodeScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> SETTING_AUTOMATON_CACHE_ENABLED = REGEXP_AUTOMATON_CACHE_ENABLED_SETTING;
    public static final Setting<ByteSizeValue> SETTING_AUTOMATON_CACHE_MAX_SIZE = REGEXP_AUTOMATON_CACHE_MAX_SIZE_SETTING;

    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();

    private volatile boolean enabled;
    private volatile long maxSizeBytes;

    private final AtomicReference<Cache<Key, CompiledAutomaton>> cacheRef = new AtomicReference<>();

    public RegexpAutomatonCache(Settings settings, ClusterService clusterService) {
        this.enabled = REGEXP_AUTOMATON_CACHE_ENABLED_SETTING.get(settings);
        this.maxSizeBytes = REGEXP_AUTOMATON_CACHE_MAX_SIZE_SETTING.get(settings).getBytes();

        if (this.enabled && this.maxSizeBytes > 0) {
            this.cacheRef.set(buildCache(this.maxSizeBytes));
        }

        if (clusterService != null) {
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(
                    REGEXP_AUTOMATON_CACHE_ENABLED_SETTING,
                    REGEXP_AUTOMATON_CACHE_MAX_SIZE_SETTING,
                    this::updateSettings
                );
        }
    }

    public RegexpAutomatonCache(Settings settings) {
        this(settings, null);
    }

    private Cache<Key, CompiledAutomaton> buildCache(long maxSizeBytes) {
        CacheBuilder<Key, CompiledAutomaton> builder = CacheBuilder.<Key, CompiledAutomaton>builder()
            .setMaximumWeight(maxSizeBytes)
            .weigher(new AutomatonWeigher())
            .removalListener(this::onRemoval);
        return builder.build();
    }

    private void onRemoval(RemovalNotification<Key, CompiledAutomaton> notification) {
        if (notification.getRemovalReason() == RemovalReason.EVICTED) {
            evictions.increment();
        }
    }

    private synchronized void updateSettings(boolean newEnabled, ByteSizeValue newMaxSize) {
        long newMaxSizeBytes = newMaxSize.getBytes();
        boolean oldEnabled = this.enabled;
        long oldMaxSizeBytes = this.maxSizeBytes;

        this.enabled = newEnabled;
        this.maxSizeBytes = newMaxSizeBytes;

        if (!newEnabled) {
            Cache<Key, CompiledAutomaton> oldCache = cacheRef.getAndSet(null);
            if (oldCache != null) {
                oldCache.invalidateAll();
            }
        } else {
            if (!oldEnabled || oldMaxSizeBytes != newMaxSizeBytes) {
                Cache<Key, CompiledAutomaton> newCache = buildCache(newMaxSizeBytes);
                Cache<Key, CompiledAutomaton> oldCache = cacheRef.getAndSet(newCache);
                if (oldCache != null) {
                    // Warm resize: copy existing entries in LRU order (oldest to newest)
                    for (Key key : oldCache.keys()) {
                        CompiledAutomaton val = oldCache.get(key);
                        if (val != null) {
                            newCache.put(key, val);
                        }
                    }
                    oldCache.invalidateAll();
                }
            }
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        updateSettings(enabled, new ByteSizeValue(maxSizeBytes));
    }

    public long getMaxSizeBytes() {
        return maxSizeBytes;
    }

    public void setMaxSize(ByteSizeValue maxSize) {
        updateSettings(enabled, maxSize);
    }

    /**
     * Gets a compiled automaton from the cache or compiles and caches it if missing.
     */
    public CompiledAutomaton getCompiledAutomaton(String pattern, int syntaxFlags, int matchFlags, int maxDeterminizedStates) {
        if (!enabled) {
            return compile(pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
        }

        Cache<Key, CompiledAutomaton> currentCache = cacheRef.get();
        if (currentCache == null) {
            return compile(pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
        }

        Key key = new Key(pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
        CompiledAutomaton cached = currentCache.get(key);
        if (cached != null) {
            hits.increment();
            return cached;
        }

        misses.increment();
        CompiledAutomaton compiled = compile(pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
        currentCache.put(key, compiled);
        return compiled;
    }

    public static CompiledAutomaton compile(String pattern, int syntaxFlags, int matchFlags, int maxDeterminizedStates) {
        RegExp regExp = new RegExp(pattern, syntaxFlags, matchFlags);
        Automaton automaton = regExp.toAutomaton(RegexpQuery.DEFAULT_PROVIDER);
        Automaton determinized = Operations.determinize(automaton, maxDeterminizedStates);
        return new CompiledAutomaton(determinized, true, false);
    }

    public long hits() {
        return hits.sum();
    }

    public long misses() {
        return misses.sum();
    }

    public long evictions() {
        return evictions.sum();
    }

    @Override
    public void close() {
        Cache<Key, CompiledAutomaton> cache = cacheRef.getAndSet(null);
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    /**
     * Cache key for compiled regex automatons.
     */
    public static class Key {
        private final String pattern;
        private final int syntaxFlags;
        private final int matchFlags;
        private final int maxDeterminizedStates;

        public Key(String pattern, int syntaxFlags, int matchFlags, int maxDeterminizedStates) {
            this.pattern = Objects.requireNonNull(pattern, "pattern cannot be null");
            this.syntaxFlags = syntaxFlags;
            this.matchFlags = matchFlags;
            this.maxDeterminizedStates = maxDeterminizedStates;
        }

        public String pattern() {
            return pattern;
        }

        public int syntaxFlags() {
            return syntaxFlags;
        }

        public int matchFlags() {
            return matchFlags;
        }

        public int maxDeterminizedStates() {
            return maxDeterminizedStates;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return syntaxFlags == key.syntaxFlags
                && matchFlags == key.matchFlags
                && maxDeterminizedStates == key.maxDeterminizedStates
                && pattern.equals(key.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, syntaxFlags, matchFlags, maxDeterminizedStates);
        }
    }

    private static class AutomatonWeigher implements ToLongBiFunction<Key, CompiledAutomaton> {
        @Override
        public long applyAsLong(Key key, CompiledAutomaton value) {
            long weight = value.ramBytesUsed();
            return weight <= 0 ? 1 : Math.min(weight, Integer.MAX_VALUE);
        }
    }
}
