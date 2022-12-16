/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class WildcardMatcher implements Predicate<String> {

    public static final WildcardMatcher ANY = new WildcardMatcher() {

        @Override
        public boolean matchAny(Stream<String> candidates) {
            return true;
        }

        @Override
        public boolean matchAny(Collection<String> candidates) {
            return true;
        }

        @Override
        public boolean matchAny(String[] candidates) {
            return true;
        }

        @Override
        public boolean matchAll(Stream<String> candidates) {
            return true;
        }

        @Override
        public boolean matchAll(Collection<String> candidates) {
            return true;
        }

        @Override
        public boolean matchAll(String[] candidates) {
            return true;
        }

        @Override
        public <T extends Collection<String>> T getMatchAny(Stream<String> candidates, Collector<String, ?, T> collector) {
            return candidates.collect(collector);
        }

        @Override
        public boolean test(String candidate) {
            return true;
        }

        @Override
        public String toString() {
            return "*";
        }
    };

    public static final WildcardMatcher NONE = new WildcardMatcher() {

        @Override
        public boolean matchAny(Stream<String> candidates) {
            return false;
        }

        @Override
        public boolean matchAny(Collection<String> candidates) {
            return false;
        }

        @Override
        public boolean matchAny(String[] candidates) {
            return false;
        }

        @Override
        public boolean matchAll(Stream<String> candidates) {
            return false;
        }

        @Override
        public boolean matchAll(Collection<String> candidates) {
            return false;
        }

        @Override
        public boolean matchAll(String[] candidates) {
            return false;
        }

        @Override
        public <T extends Collection<String>> T getMatchAny(Stream<String> candidates, Collector<String, ?, T> collector) {
            return Stream.<String>empty().collect(collector);
        }

        @Override
        public <T extends Collection<String>> T getMatchAny(Collection<String> candidate, Collector<String, ?, T> collector) {
            return Stream.<String>empty().collect(collector);
        }

        @Override
        public <T extends Collection<String>> T getMatchAny(String[] candidate, Collector<String, ?, T> collector) {
            return Stream.<String>empty().collect(collector);
        }

        @Override
        public boolean test(String candidate) {
            return false;
        }

        @Override
        public String toString() {
            return "<NONE>";
        }
    };

    public static WildcardMatcher from(String pattern, boolean caseSensitive) {
        if (pattern.equals("*")) {
            return ANY;
        } else if (pattern.startsWith("/") && pattern.endsWith("/")) {
            return new RegexMatcher(pattern, caseSensitive);
        } else if (pattern.indexOf('?') >= 0 || pattern.indexOf('*') >= 0) {
            return caseSensitive ? new SimpleMatcher(pattern) : new CasefoldingMatcher(pattern, SimpleMatcher::new);
        } else {
            return caseSensitive ? new Exact(pattern) : new CasefoldingMatcher(pattern, Exact::new);
        }
    }

    public static WildcardMatcher from(String pattern) {
        return from(pattern, true);
    }

    // This may in future use more optimized techniques to combine multiple WildcardMatchers in a single automaton
    public static <T> WildcardMatcher from(Stream<T> stream, boolean caseSensitive) {
        Collection<WildcardMatcher> matchers = stream.map(t -> {
            if (t instanceof String) {
                return WildcardMatcher.from(((String) t), caseSensitive);
            } else if (t instanceof WildcardMatcher) {
                return ((WildcardMatcher) t);
            }
            throw new UnsupportedOperationException("WildcardMatcher can't be constructed from " + t.getClass().getSimpleName());
        }).collect(Collectors.toSet());

        if (matchers.isEmpty()) {
            return NONE;
        } else if (matchers.size() == 1) {
            return matchers.stream().findFirst().get();
        }
        return new MatcherCombiner(matchers);
    }

    public static <T> WildcardMatcher from(Collection<T> collection, boolean caseSensitive) {
        if (collection == null || collection.isEmpty()) {
            return NONE;
        } else if (collection.size() == 1) {
            T t = collection.stream().findFirst().get();
            if (t instanceof String) {
                return from(((String) t), caseSensitive);
            } else if (t instanceof WildcardMatcher) {
                return ((WildcardMatcher) t);
            }
            throw new UnsupportedOperationException("WildcardMatcher can't be constructed from " + t.getClass().getSimpleName());
        }
        return from(collection.stream(), caseSensitive);
    }

    public static WildcardMatcher from(String[] patterns, boolean caseSensitive) {
        if (patterns == null || patterns.length == 0) {
            return NONE;
        } else if (patterns.length == 1) {
            return from(patterns[0], caseSensitive);
        }
        return from(Arrays.stream(patterns), caseSensitive);
    }

    public static WildcardMatcher from(Stream<String> patterns) {
        return from(patterns, true);
    }

    public static WildcardMatcher from(Collection<?> patterns) {
        return from(patterns, true);
    }

    public static WildcardMatcher from(String... patterns) {
        return from(patterns, true);
    }

    public WildcardMatcher concat(Stream<WildcardMatcher> matchers) {
        return new WildcardMatcher.MatcherCombiner(Stream.concat(matchers, Stream.of(this)).collect(Collectors.toSet()));
    }

    public WildcardMatcher concat(Collection<WildcardMatcher> matchers) {
        if (matchers.isEmpty()) {
            return this;
        }
        return concat(matchers.stream());
    }

    public WildcardMatcher concat(WildcardMatcher... matchers) {
        if (matchers.length == 0) {
            return this;
        }
        return concat(Arrays.stream(matchers));
    }

    public boolean matchAny(Stream<String> candidates) {
        return candidates.anyMatch(this);
    }

    public boolean matchAny(Collection<String> candidates) {
        return matchAny(candidates.stream());
    }

    public boolean matchAny(String... candidates) {
        return matchAny(Arrays.stream(candidates));
    }

    public boolean matchAll(Stream<String> candidates) {
        return candidates.allMatch(this);
    }

    public boolean matchAll(Collection<String> candidates) {
        return matchAll(candidates.stream());
    }

    public boolean matchAll(String[] candidates) {
        return matchAll(Arrays.stream(candidates));
    }

    public <T extends Collection<String>> T getMatchAny(Stream<String> candidates, Collector<String, ?, T> collector) {
        return candidates.filter(this).collect(collector);
    }

    public <T extends Collection<String>> T getMatchAny(Collection<String> candidate, Collector<String, ?, T> collector) {
        return getMatchAny(candidate.stream(), collector);
    }

    public <T extends Collection<String>> T getMatchAny(final String[] candidate, Collector<String, ?, T> collector) {
        return getMatchAny(Arrays.stream(candidate), collector);
    }

    public Optional<WildcardMatcher> findFirst(final String candidate) {
        return Optional.ofNullable(test(candidate) ? this : null);
    }

    public static List<WildcardMatcher> matchers(Collection<String> patterns) {
        return patterns.stream().map(p -> WildcardMatcher.from(p, true)).collect(Collectors.toList());
    }

    public static List<String> getAllMatchingPatterns(final Collection<WildcardMatcher> matchers, final String candidate) {
        return matchers.stream().filter(p -> p.test(candidate)).map(Objects::toString).collect(Collectors.toList());
    }

    public static List<String> getAllMatchingPatterns(final Collection<WildcardMatcher> pattern, final Collection<String> candidates) {
        return pattern.stream().filter(p -> p.matchAny(candidates)).map(Objects::toString).collect(Collectors.toList());
    }

    //
    // --- Implementation specializations ---
    //
    // Casefolding matcher - sits on top of case-sensitive matcher
    // and proxies toLower() of input string to the wrapped matcher
    private static final class CasefoldingMatcher extends WildcardMatcher {

        private final WildcardMatcher inner;

        public CasefoldingMatcher(String pattern, Function<String, WildcardMatcher> simpleWildcardMatcher) {
            this.inner = simpleWildcardMatcher.apply(pattern.toLowerCase());
        }

        @Override
        public boolean test(String candidate) {
            return inner.test(candidate.toLowerCase());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CasefoldingMatcher that = (CasefoldingMatcher) o;
            return inner.equals(that.inner);
        }

        @Override
        public int hashCode() {
            return inner.hashCode();
        }

        @Override
        public String toString() {
            return inner.toString();
        }
    }

    public static final class Exact extends WildcardMatcher {

        private final String pattern;

        private Exact(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean test(String candidate) {
            return pattern.equals(candidate);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Exact that = (Exact) o;
            return pattern.equals(that.pattern);
        }

        @Override
        public int hashCode() {
            return pattern.hashCode();
        }

        @Override
        public String toString() {
            return pattern;
        }
    }

    // RegexMatcher uses JDK Pattern to test for matching,
    // assumes "/<regex>/" strings as input pattern
    private static final class RegexMatcher extends WildcardMatcher {

        private final Pattern pattern;

        private RegexMatcher(String pattern, boolean caseSensitive) {
            if (!(pattern.length() > 1 && pattern.startsWith("/") && pattern.endsWith("/"))) {
                throw new IllegalArgumentException();
            }
            final String stripSlashesPattern = pattern.substring(1, pattern.length() - 1);
            this.pattern = caseSensitive
                ? Pattern.compile(stripSlashesPattern)
                : Pattern.compile(stripSlashesPattern, Pattern.CASE_INSENSITIVE);
        }

        @Override
        public boolean test(String candidate) {
            return pattern.matcher(candidate).matches();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegexMatcher that = (RegexMatcher) o;
            return pattern.pattern().equals(that.pattern.pattern());
        }

        @Override
        public int hashCode() {
            return pattern.pattern().hashCode();
        }

        @Override
        public String toString() {
            return "/" + pattern.pattern() + "/";
        }
    }

    // Simple implementation of WildcardMatcher matcher with * and ? without
    // using exlicit stack or recursion (as long as we don't need sub-matches it does work)
    // allows us to save on resources and heap allocations unless Regex is required
    private static final class SimpleMatcher extends WildcardMatcher {

        private final String pattern;

        SimpleMatcher(String pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean test(String candidate) {
            int i = 0;
            int j = 0;
            int n = candidate.length();
            int m = pattern.length();
            int text_backup = -1;
            int wild_backup = -1;
            while (i < n) {
                if (j < m && pattern.charAt(j) == '*') {
                    text_backup = i;
                    wild_backup = ++j;
                } else if (j < m && (pattern.charAt(j) == '?' || pattern.charAt(j) == candidate.charAt(i))) {
                    i++;
                    j++;
                } else {
                    if (wild_backup == -1) return false;
                    i = ++text_backup;
                    j = wild_backup;
                }
            }
            while (j < m && pattern.charAt(j) == '*')
                j++;
            return j >= m;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleMatcher that = (SimpleMatcher) o;
            return pattern.equals(that.pattern);
        }

        @Override
        public int hashCode() {
            return pattern.hashCode();
        }

        @Override
        public String toString() {
            return pattern;
        }
    }

    // MatcherCombiner is a combination of a set of matchers
    // matches if any of the set do
    // Empty MultiMatcher always returns false
    private static final class MatcherCombiner extends WildcardMatcher {

        private final Collection<WildcardMatcher> wildcardMatchers;
        private final int hashCode;

        MatcherCombiner(Collection<WildcardMatcher> wildcardMatchers) {
            if (wildcardMatchers.size() == 0) {
                throw new IllegalArgumentException();
            }
            this.wildcardMatchers = wildcardMatchers;
            hashCode = wildcardMatchers.hashCode();
        }

        @Override
        public boolean test(String candidate) {
            return wildcardMatchers.stream().anyMatch(m -> m.test(candidate));
        }

        @Override
        public Optional<WildcardMatcher> findFirst(final String candidate) {
            return wildcardMatchers.stream().filter(m -> m.test(candidate)).findFirst();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MatcherCombiner that = (MatcherCombiner) o;
            return wildcardMatchers.equals(that.wildcardMatchers);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return wildcardMatchers.toString();
        }
    }
}
