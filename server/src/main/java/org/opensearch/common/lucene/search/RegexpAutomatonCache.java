/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Process-wide LRU cache for compiled regex {@link CompiledAutomaton} objects.
 *
 * <p>Building an automaton from a regex pattern (via {@link RegExp#toAutomaton}) and then
 * compiling it into a {@link CompiledAutomaton} (determinization + UTF32-to-UTF8 transition
 * table construction) are the two most expensive steps during regex query parsing. This cache
 * stores the fully compiled form so that subsequent queries with the same pattern + flags skip
 * both NFA construction and DFA compilation.
 *
 * <p>The cache is static and shared across all queries on the JVM. {@link CompiledAutomaton}
 * instances are immutable, so the same object can be safely reused from concurrent threads.
 *
 * <p>The cache is keyed on {@code (pattern, syntaxFlags, matchFlags, determinizeWorkLimit)}.
 * Callers that pass a custom {@link AutomatonProvider} (anything other than
 * {@link RegexpQuery#DEFAULT_PROVIDER}) bypass the cache, since a named-automaton provider may
 * produce a different automaton for the same pattern.
 *
 * @opensearch.internal
 */
public final class RegexpAutomatonCache {

    /**
     * Whether the regex automaton cache is enabled. When disabled, every regex query compiles its
     * automaton from scratch. Toggling this off also invalidates the existing cache contents.
     */
    public static final Setting<Boolean> CACHE_ENABLED_SETTING = Setting.boolSetting(
        "search.regexp.automaton_cache.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum RAM, in bytes, retained by cached {@link CompiledAutomaton} entries before LRU
     * eviction. Compiled automaton size varies widely with pattern complexity, so the cache is
     * bounded by actual memory footprint ({@link CompiledAutomaton#ramBytesUsed()}) rather than
     * entry count -- a small number of complex patterns could otherwise consume disproportionate
     * memory under a count-based limit.
     */
    public static final Setting<ByteSizeValue> CACHE_MAX_SIZE_SETTING = Setting.memorySizeSetting(
        "search.regexp.automaton_cache.size",
        new ByteSizeValue(50, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final RegexpAutomatonCache INSTANCE = new RegexpAutomatonCache(CACHE_MAX_SIZE_SETTING.get(Settings.EMPTY).getBytes());

    private final Cache<Key, CompiledAutomaton> cache;
    private volatile boolean enabled = CACHE_ENABLED_SETTING.get(Settings.EMPTY);

    RegexpAutomatonCache(long maxSizeBytes) {
        // Keys are not weighed: the pattern string is bounded by index.max_regex_length (1000 chars by default),
        // which is negligible next to the compiled automaton.
        this.cache = CacheBuilder.<Key, CompiledAutomaton>builder()
            .setMaximumWeight(maxSizeBytes)
            .weigher((k, v) -> v.ramBytesUsed())
            .build();
    }

    public static RegexpAutomatonCache getInstance() {
        return INSTANCE;
    }

    /**
     * Updates the byte budget in place, as {@code IndicesFieldDataCache} does for its size setting.
     * Cached entries are kept; when the budget shrinks, least-recently-used entries are evicted
     * until the cache fits. Eviction runs on the calling thread, which is acceptable because the
     * cache is bounded to a few tens of megabytes.
     */
    public void resize(long maxSizeBytes) {
        long oldMaxSizeBytes = cache.getMaximumWeight();
        cache.setMaximumWeight(maxSizeBytes);
        if (maxSizeBytes < oldMaxSizeBytes) {
            cache.refresh();
        }
    }

    /**
     * Enables or disables the cache. Disabling invalidates every cached entry, so re-enabling
     * starts from an empty cache. Hit, miss and eviction counters are not reset.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled == false) {
            cache.invalidateAll();
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** Number of entries currently held in the cache. */
    public long count() {
        return cache.count();
    }

    /** Current total RAM, in bytes, retained by cached {@link CompiledAutomaton} entries. */
    public long ramBytesUsed() {
        return cache.weight();
    }

    /** Configured maximum RAM, in bytes, the cache is allowed to retain. */
    public long maxSizeBytes() {
        return cache.getMaximumWeight();
    }

    /** Cache hit count since process start. */
    public long hits() {
        return cache.stats().getHits();
    }

    /** Cache miss count since process start. */
    public long misses() {
        return cache.stats().getMisses();
    }

    /** Cache eviction count since process start. */
    public long evictions() {
        return cache.stats().getEvictions();
    }

    private static CompiledAutomaton buildCompiledAutomaton(
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int determinizeWorkLimit,
        AutomatonProvider provider
    ) {
        Automaton automaton = Operations.determinize(
            new RegExp(pattern, syntaxFlags, matchFlags).toAutomaton(provider),
            determinizeWorkLimit
        );
        return new CompiledAutomaton(automaton, false, true, false);
    }

    /**
     * Returns a fully compiled {@link CompiledAutomaton} for the supplied regex pattern,
     * using a cached entry when possible.
     *
     * <p>When the cache is disabled, or when the caller passes a non-default
     * {@link AutomatonProvider}, this compiles a fresh automaton and does not touch the cache.
     */
    public CompiledAutomaton getCompiledAutomaton(
        String pattern,
        int syntaxFlags,
        int matchFlags,
        int determinizeWorkLimit,
        AutomatonProvider provider
    ) {
        if (enabled == false || provider != RegexpQuery.DEFAULT_PROVIDER) {
            return buildCompiledAutomaton(pattern, syntaxFlags, matchFlags, determinizeWorkLimit, provider);
        }
        Key key = new Key(pattern, syntaxFlags, matchFlags, determinizeWorkLimit);
        try {
            return cache.computeIfAbsent(
                key,
                k -> buildCompiledAutomaton(k.pattern, k.syntaxFlags, k.matchFlags, k.determinizeWorkLimit, provider)
            );
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TooComplexToDeterminizeException) {
                throw (TooComplexToDeterminizeException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException("Unexpected checked exception while building automaton for pattern [" + pattern + "]", cause);
        }
    }

    /**
     * Convenience overload that uses {@link RegexpQuery#DEFAULT_PROVIDER}.
     */
    public CompiledAutomaton getCompiledAutomaton(String pattern, int syntaxFlags, int matchFlags, int determinizeWorkLimit) {
        return getCompiledAutomaton(pattern, syntaxFlags, matchFlags, determinizeWorkLimit, RegexpQuery.DEFAULT_PROVIDER);
    }

    /**
     * Builds a regexp query over {@code term.field()} whose pattern is {@code term.text()}, matching
     * the semantics of Lucene's {@link RegexpQuery}: callers must pass the term after any search-time
     * normalization so the automaton is compiled from the normalized pattern.
     *
     * <p>When the cache is disabled this returns a plain {@link RegexpQuery}, so the default code path
     * is unchanged; otherwise it returns a {@link PrecompiledAutomatonQuery} backed by a cached automaton.
     */
    public MultiTermQuery regexpQuery(
        Term term,
        int syntaxFlags,
        int matchFlags,
        int determinizeWorkLimit,
        MultiTermQuery.RewriteMethod rewriteMethod
    ) {
        if (enabled == false) {
            return new RegexpQuery(term, syntaxFlags, matchFlags, RegexpQuery.DEFAULT_PROVIDER, determinizeWorkLimit, rewriteMethod);
        }
        String pattern = term.text();
        CompiledAutomaton compiled = getCompiledAutomaton(pattern, syntaxFlags, matchFlags, determinizeWorkLimit);
        return new PrecompiledAutomatonQuery(term, compiled, pattern, rewriteMethod);
    }

    /**
     * Composite cache key.
     *
     * <p>{@code determinizeWorkLimit} is included even though it is a safety guard (upper bound
     * on DFA states) rather than a parameter that changes the resulting automaton. Removing it
     * would improve cache hit rates (same pattern with different limits would share one entry),
     * but it would also change observable behavior: a query with a low limit that would normally
     * throw {@link TooComplexToDeterminizeException} could silently succeed if a previous query
     * with a higher limit already cached the result. We preserve the existing semantics — if
     * the caller's limit is too low, they should get an exception, not a cached success.
     */
    static final class Key {
        final String pattern;
        final int syntaxFlags;
        final int matchFlags;
        final int determinizeWorkLimit;
        private final int hash;

        Key(String pattern, int syntaxFlags, int matchFlags, int determinizeWorkLimit) {
            this.pattern = pattern;
            this.syntaxFlags = syntaxFlags;
            this.matchFlags = matchFlags;
            this.determinizeWorkLimit = determinizeWorkLimit;
            this.hash = Objects.hash(pattern, syntaxFlags, matchFlags, determinizeWorkLimit);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Key == false) {
                return false;
            }
            Key key = (Key) o;
            return syntaxFlags == key.syntaxFlags
                && matchFlags == key.matchFlags
                && determinizeWorkLimit == key.determinizeWorkLimit
                && pattern.equals(key.pattern);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
