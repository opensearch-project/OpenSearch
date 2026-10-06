/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Rejects requests using {@link SearchSourceBuilder} features this path does not implement,
 * instead of silently ignoring them (an ignored {@code search_after} re-serves page one with
 * HTTP 200 — a loud 400 is the honest answer).
 *
 * <p>Not rejected: the supported parameters ({@code SearchSourceConverter} converts them) and
 * three no-op hints — {@code timeout} (best-effort in classic search too; Dashboards sends it
 * routinely), {@code track_scores} (scores are null on this path regardless), {@code stats}
 * (telemetry tag). The classification is enforced by
 * {@code UnsupportedSearchParametersTests}: it enumerates {@code SearchSourceBuilder}'s public
 * API and fails when a new core feature is left unclassified.
 */
final class UnsupportedSearchParameters {

    /** One rejected parameter: its REST name and the predicate that detects it on a request. */
    private record Check(String restName, Predicate<SearchSourceBuilder> isSet) {
    }

    private static final List<Check> REJECTED = List.of(
        new Check("post_filter", s -> s.postFilter() != null),
        new Check("explain", s -> Boolean.TRUE.equals(s.explain())),
        new Check("version", s -> Boolean.TRUE.equals(s.version())),
        new Check("seq_no_primary_term", s -> Boolean.TRUE.equals(s.seqNoAndPrimaryTerm())),
        new Check("include_named_queries_score", SearchSourceBuilder::includeNamedQueriesScore),
        new Check("search_after", s -> s.searchAfter() != null),
        new Check("slice", s -> s.slice() != null),
        new Check("min_score", s -> s.minScore() != null),
        new Check("terminate_after", s -> s.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER),
        new Check("stored_fields", s -> s.storedFields() != null),
        new Check("docvalue_fields", s -> s.docValueFields() != null && !s.docValueFields().isEmpty()),
        new Check("script_fields", s -> s.scriptFields() != null && !s.scriptFields().isEmpty()),
        new Check(
            "derived",
            s -> (s.getDerivedFieldsObject() != null && !s.getDerivedFieldsObject().isEmpty())
                || (s.getDerivedFields() != null && !s.getDerivedFields().isEmpty())
        ),
        new Check("fields", s -> s.fetchFields() != null && !s.fetchFields().isEmpty()),
        new Check("highlight", s -> s.highlighter() != null),
        new Check("suggest", s -> s.suggest() != null),
        new Check("rescore", s -> s.rescores() != null && !s.rescores().isEmpty()),
        new Check("indices_boost", s -> s.indexBoosts() != null && !s.indexBoosts().isEmpty()),
        new Check("ext", s -> s.ext() != null && !s.ext().isEmpty()),
        new Check("profile", SearchSourceBuilder::profile),
        new Check("collapse", s -> s.collapse() != null),
        new Check("pit", s -> s.pointInTimeBuilder() != null),
        new Check(
            "search_pipeline",
            s -> s.pipeline() != null || (s.searchPipelineSource() != null && !s.searchPipelineSource().isEmpty())
        ),
        new Check("verbose_pipeline", s -> Boolean.TRUE.equals(s.verbosePipeline()))
    );

    private UnsupportedSearchParameters() {}

    /**
     * Throws when the request uses any unsupported feature, naming every offender at once so
     * clients fix their request in one round trip. The transport maps
     * {@link ConversionException} to HTTP 400.
     */
    static void reject(SearchSourceBuilder searchSource) throws ConversionException {
        List<String> offending = new ArrayList<>();
        for (Check check : REJECTED) {
            if (check.isSet.test(searchSource)) {
                offending.add(check.restName);
            }
        }
        if (!offending.isEmpty()) {
            throw new ConversionException("Parameters not supported on this search path: " + offending);
        }
    }
}
