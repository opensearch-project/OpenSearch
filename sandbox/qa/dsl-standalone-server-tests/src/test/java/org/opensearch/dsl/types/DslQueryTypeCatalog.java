/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import org.opensearch.analytics.qa.Dataset;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Catalog of DSL query types, one entry per {@code resources/datasets/<type>/} folder.
 *
 * <p>Each entry names a query-type folder (its {@link Dataset}: {@code mapping.json} + {@code bulk.json}
 * + {@code dsl/q{N}.json}). {@link DslQueryTypesIT} turns every {@code (entry, queryNumber)} pair into
 * an <b>independent</b> parameterized test that provisions the type's parquet index, runs the query,
 * and validates the response against a golden file ({@code dsl/expected/q{N}.json}).
 *
 * <p>The suite's purpose is to map what the parquet/composite engine can and cannot serve. Every type
 * is treated as <b>expected-to-work</b>: a type is green only when its live response matches its golden.
 * Types the engine cannot handle today — geo/nested field mappings (rejected at index creation),
 * multi-valued keyword arrays (rejected at ingest) — therefore surface as <b>red</b>, which is the
 * finding we want to expose, not hide. There is no per-type "expected outcome" guesswork: the golden
 * is the single source of truth.
 *
 * <p>Known engine gaps can be tagged on their {@link Entry} (see {@link #TAG_MULTI_VALUED_KEYWORD});
 * {@link DslQueryTypesIT} then reports those parameters as skipped-with-reason instead of red, so the
 * suite stays green while the finding stays visible — remove the tag to reactivate the test.
 */
public final class DslQueryTypeCatalog {

    /**
     * Marks datasets whose {@code tags} keyword field is a multi-valued array, which the parquet
     * backend rejects at ingest / fails at query today. Entries carrying this tag are skipped by
     * {@link DslQueryTypesIT} until the engine supports them — remove the tag to reactivate the test.
     */
    public static final String TAG_MULTI_VALUED_KEYWORD = "multi-valued-keyword";

    private DslQueryTypeCatalog() {}

    /** One query-type entry: its dataset folder/index and query family. */
    public static final class Entry {
        /** Query-type key == folder name under {@code resources/datasets/}. */
        public final String type;
        /** Query family (term-level, full-text, compound, scoring, span, relational, geo, specialized). */
        public final String family;
        /** Dataset descriptor: folder name == index name == {@link #type}. */
        public final Dataset dataset;
        /**
         * Tags marking known engine gaps (e.g. {@link #TAG_MULTI_VALUED_KEYWORD}); tagged entries are
         * skipped by {@link DslQueryTypesIT}. Unmodifiable and insertion-ordered.
         */
        public final Set<String> tags;

        Entry(String type, String family) {
            this(type, family, Collections.emptySet());
        }

        Entry(String type, String family, Set<String> tags) {
            this.type = type;
            this.family = family;
            this.dataset = new Dataset(type, type);
            this.tags = Collections.unmodifiableSet(new LinkedHashSet<>(tags));
        }

        /**
         * Returns a NEW entry with {@code moreTags} unioned into this entry's tags. Fields are final,
         * so nothing is mutated in place.
         */
        public Entry tagged(String... moreTags) {
            Set<String> union = new LinkedHashSet<>(this.tags);
            Collections.addAll(union, moreTags);
            return new Entry(this.type, this.family, union);
        }

        /** True when this entry carries at least one tag (a known engine gap). */
        public boolean hasTags() {
            return tags.isEmpty() == false;
        }
    }

    private static Entry e(String type, String family) {
        return new Entry(type, family);
    }

    /** All catalogued query types, aligned 1:1 with the {@code resources/datasets/<type>/} folders. */
    public static List<Entry> all() {
        return List.of(
            // ── term-level ──
            e("term", "term-level").tagged(TAG_MULTI_VALUED_KEYWORD),

            // ── general keyword dataset (people: 5 docs; keyword/int/double/text fields) ──
            // Exercises term queries returning multiple docs, a single doc, and no docs against a
            // plain (non-tags) keyword dataset. Golden answers are the true Lucene results.
            e("people", "term-level"),

            // ── compound ──
            e("bool", "compound").tagged(TAG_MULTI_VALUED_KEYWORD),

            // ── single-valued-tags variants (probe parquet's multi-value keyword limitation) ──
            // Same mapping + same query as the parent type, but with `tags` reduced from a multi-valued
            // array to a bare scalar. The parent types fail on parquet at ingest — "Cannot accept multiple
            // values for field [tags] of type [keyword]" — so these variants isolate that as the sole
            // cause: with scalar tags the docs ingest and the parent's query is exercised normally.
            e("term_scalar", "term-level"),
            e("bool_scalar", "compound")
        );
    }
}
