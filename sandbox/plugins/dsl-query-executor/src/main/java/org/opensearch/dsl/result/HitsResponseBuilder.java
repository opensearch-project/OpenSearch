/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a HITS execution result into {@link SearchHits}.
 *
 * <p>Each row is one document: the plan row type's column names paired with the row's cells
 * are re-assembled into a {@code _source} map (dotted column names re-nested into objects,
 * null cells omitted — matching how object fields were flattened into schema columns).
 *
 * <p>Legacy-compat choices, constrained by what the analytics engine exposes today:
 * <ul>
 *   <li>{@code _id} is populated from the row's {@code _id} metadata column (the stored
 *       Uid-encoded bytes; requested by the plan via {@code IdColumnSchema}). The column is
 *       lifted out of the row and never appears in {@code _source}. Rows without the column
 *       render {@code _id} null.</li>
 *   <li>{@code _score}/{@code max_score} are {@link Float#NaN} (rendered {@code null}): the
 *       engine has no relevance scoring, matching legacy's non-scored (field-sorted) responses.</li>
 *   <li>{@code hits.total}: classic {@code track_total_hits} semantics. The COUNT plan's
 *       {@code COUNT(*)} (merged into {@link CountTotals}) is reported exact ({@code eq}) up
 *       to the threshold and clamped to a {@code gte} lower bound past it; tracking disabled
 *       omits the total. Without a count, the total is inferred from the returned page: a
 *       full page means "at least this many" ({@code gte}) and a short page is exact
 *       ({@code eq}).</li>
 * </ul>
 */
public final class HitsResponseBuilder {

    private HitsResponseBuilder() {}

    /**
     * Builds {@link SearchHits} from the HITS execution result, if any.
     *
     * @param results all execution results; at most one is a {@link QueryPlans.Type#HITS} result
     * @param request the original search request ({@code size} drives truncation and the total relation)
     * @param countTotals merged COUNT plan results supplying the {@code COUNT(*)} total, or null
     *        when no count plan ran
     * @return the search hits section of the response
     * @throws ConversionException if a row cannot be assembled into a source document
     */
    public static SearchHits build(List<ExecutionResult> results, SearchRequest request, CountTotals countTotals)
        throws ConversionException {
        ExecutionResult hitsResult = null;
        for (ExecutionResult result : results) {
            if (result.getType() == QueryPlans.Type.HITS) {
                hitsResult = result;
            }
        }

        if (hitsResult == null) {
            // size=0 request: no hits were fetched. The COUNT plan's total supplies the exact
            // match count like legacy; without one (tracking disabled, or results not produced
            // by the converter, e.g. in tests) fall back to omitted / the honest lower bound.
            return new SearchHits(new SearchHit[0], resolveTotal(request, countTotals, null), Float.NaN);
        }

        int size = resolveSize(request);
        List<String> fieldNames = hitsResult.getFieldNames();
        int idOrdinal = fieldNames.indexOf(IdFieldMapper.NAME);
        List<Object[]> rows = new ArrayList<>();
        for (Object[] row : hitsResult.getRows()) {
            rows.add(row);
        }

        // The plan carries fetch=size, but truncate defensively in case the engine returned more.
        int hitCount = Math.min(rows.size(), size);
        SearchHit[] hits = new SearchHit[hitCount];
        for (int i = 0; i < hitCount; i++) {
            hits[i] = buildHit(i, fieldNames, rows.get(i), idOrdinal);
        }

        return new SearchHits(hits, resolveTotal(request, countTotals, rows.size()), Float.NaN);
    }

    /**
     * Resolves {@code hits.total} with classic {@code track_total_hits} semantics:
     * omitted when tracking is disabled; the COUNT plan's {@code COUNT(*)} rendered exact
     * ({@code eq}) up to the threshold and as a {@code gte} lower bound past it; page
     * inference (short page exact / full page lower bound) when no count ran.
     */
    private static TotalHits resolveTotal(SearchRequest request, CountTotals countTotals, Integer pageRows) {
        SearchSourceBuilder source = request.source();
        Integer trackUpTo = source == null ? null : source.trackTotalHitsUpTo();
        int threshold = trackUpTo == null ? SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO : trackUpTo;
        if (threshold == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            return null; // track_total_hits: false — total omitted, like classic search
        }
        if (countTotals != null && countTotals.totalDocs() != null) {
            long count = countTotals.totalDocs();
            return count <= threshold
                ? new TotalHits(count, TotalHits.Relation.EQUAL_TO)
                : new TotalHits(threshold, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        if (pageRows == null) {
            // size=0 without a count result (converter-independent callers, e.g. tests)
            return new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        }
        // eq/gte page inference: see class javadoc.
        TotalHits.Relation relation = pageRows < resolveSize(request)
            ? TotalHits.Relation.EQUAL_TO
            : TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        return new TotalHits(pageRows, relation);
    }

    private static int resolveSize(SearchRequest request) {
        SearchSourceBuilder source = request.source();
        return source != null && source.size() != -1 ? source.size() : SearchService.DEFAULT_SIZE;
    }

    /** Decodes the {@code _id} cell: Uid-encoded bytes from the engine, or a plain string in tests. */
    private static String decodeId(Object cell) throws ConversionException {
        if (cell == null) {
            return null;
        }
        if (cell instanceof byte[] bytes) {
            return Uid.decodeId(bytes);
        }
        if (cell instanceof String s) {
            return s;
        }
        throw new ConversionException("Unsupported _id cell type: " + cell.getClass().getName());
    }

    private static SearchHit buildHit(int docId, List<String> fieldNames, Object[] row, int idOrdinal) throws ConversionException {
        Map<String, Object> source = buildSourceMap(fieldNames, row, idOrdinal);
        // Rows without the _id column render the hit without an _id field (id = null).
        String id = idOrdinal >= 0 ? decodeId(row[idOrdinal]) : null;
        SearchHit hit = new SearchHit(docId, id, null, null);
        hit.score(Float.NaN);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(source);
            hit.sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new ConversionException("Failed to serialize hit _source", e);
        }
        return hit;
    }

    /**
     * Re-assembles a row into a source map. Dotted column names (the schema flattens object
     * fields into {@code parent.child} leaf columns) are re-nested into object maps so the
     * rendered {@code _source} matches the legacy document shape. Null cells are omitted —
     * after the columnar round trip an absent field and an explicit null are indistinguishable.
     */
    static Map<String, Object> buildSourceMap(List<String> fieldNames, Object[] row, int idOrdinal) throws ConversionException {
        if (fieldNames.size() != row.length) {
            throw new ConversionException(
                "HITS row has " + row.length + " cells but the plan row type declares " + fieldNames.size() + " columns"
            );
        }
        Map<String, Object> source = new LinkedHashMap<>();
        for (int i = 0; i < row.length; i++) {
            // _id lives in the hit envelope, never in _source; null cells are omitted.
            if (i == idOrdinal || row[i] == null) {
                continue;
            }
            insertNested(source, fieldNames.get(i), row[i]);
        }
        return source;
    }

    @SuppressWarnings("unchecked")
    private static void insertNested(Map<String, Object> root, String dottedName, Object value) throws ConversionException {
        String[] parts = dottedName.split("\\.");
        Map<String, Object> current = root;
        for (int i = 0; i < parts.length - 1; i++) {
            Object child = current.get(parts[i]);
            if (child == null) {
                Map<String, Object> next = new LinkedHashMap<>();
                current.put(parts[i], next);
                current = next;
            } else if (child instanceof Map) {
                current = (Map<String, Object>) child;
            } else {
                throw new ConversionException("Column '" + dottedName + "' conflicts with scalar column '" + parts[i] + "'");
            }
        }
        Object previous = current.put(parts[parts.length - 1], value);
        if (previous != null) {
            throw new ConversionException("Column '" + dottedName + "' conflicts with an object column of the same path");
        }
    }
}
