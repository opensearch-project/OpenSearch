/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.common.Table;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds a grouped + aggregated view of a {@link Table} for {@code _cat} APIs. Grouping and
 * aggregation are both expressed entirely through the {@code h=} parameter: any bare column token
 * (e.g. {@code index}) is treated as a GROUP BY key, and any function token (e.g. {@code sum(docs)})
 * is treated as an aggregate over the grouped rows. This mirrors the SQL mental model
 * {@code SELECT index, sum(docs) ... GROUP BY index} without a separate {@code summarize=} parameter.
 *
 * <p>When {@code h=} contains no aggregation function the table is returned unchanged, so ordinary
 * {@code h=} listings behave exactly as they did before summarization existed.
 *
 * <p>Supported functions: {@code sum}, {@code count}, {@code avg}, {@code min}, {@code max}.
 * Numeric types {@link Long}, {@link Integer}, {@link Short}, {@link Byte}, {@link ByteSizeValue},
 * {@link SizeValue}, {@link TimeValue} are preserved through aggregation so downstream rendering
 * with {@code bytes=}/{@code time=}/{@code size=} continues to work on aggregated values.
 *
 * <p>The {@code avg} function computes {@code sum / count_of_non_null_values}; rows whose value is
 * null for a given aggregation column do not contribute to either the numerator or denominator
 * for that column's average.
 *
 * @opensearch.internal
 */
public final class TableSummarizer {

    /** Matches function syntax like {@code sum(docs.count)}, {@code count(shard)}. Case-insensitive so {@code SUM(docs)} also matches. */
    private static final Pattern AGG_FUNC_PATTERN = Pattern.compile("^(sum|count|avg|min|max)\\((.+)\\)$", Pattern.CASE_INSENSITIVE);

    private TableSummarizer() {}

    /**
     * Returns true if at least one token in the {@code h=} parameter is an aggregation function
     * invocation like {@code sum(field)}. Used by callers that want to short-circuit upstream row
     * construction when neither sort nor aggregation is requested.
     */
    public static boolean hasAggregation(String headersParam) {
        if (headersParam == null || headersParam.isEmpty()) {
            return false;
        }
        for (String header : Strings.splitStringByCommaToArray(headersParam)) {
            if (AGG_FUNC_PATTERN.matcher(header.trim()).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Build a summarized table by inferring grouping and aggregation from the {@code h=} parameter.
     * Bare column tokens become GROUP BY keys; {@code func(field)} tokens become aggregates. Returns
     * the original {@code table} unchanged when {@code hParam} is null/empty or contains no
     * aggregation function (i.e. an ordinary listing was requested).
     *
     * @param hParam value of the {@code h=} query parameter; may be null
     */
    public static Table summarize(Table table, String hParam) {
        if (hParam == null || hParam.isEmpty()) {
            return table;
        }
        Map<String, String> aliasMap = table.getAliasMap();

        // Split h= into group-by columns (bare tokens) and aggregation columns (func(field) tokens).
        List<String> groupByTokens = new ArrayList<>();
        List<AggColumn> aggColumns = new ArrayList<>();
        for (String token : Strings.splitStringByCommaToArray(hParam)) {
            String h = token.trim();
            if (h.isEmpty()) {
                continue;
            }
            Matcher m = AGG_FUNC_PATTERN.matcher(h);
            if (m.matches()) {
                String func = m.group(1).toLowerCase(Locale.ROOT);
                String field = aliasMap.getOrDefault(m.group(2).trim(), m.group(2).trim());
                aggColumns.add(new AggColumn(func, field, h));
            } else {
                groupByTokens.add(h);
            }
        }

        // No aggregation requested => ordinary listing; return the table unchanged so that plain
        // h= requests behave exactly as they did before summarization existed.
        if (aggColumns.isEmpty()) {
            return table;
        }

        // Resolve group-by field aliases to canonical names. The raw token is kept for the result
        // header display name so it still matches the h= token during downstream column filtering.
        String[] groupByFields = groupByTokens.toArray(new String[0]);
        String[] resolvedGroupBy = new String[groupByFields.length];
        for (int i = 0; i < groupByFields.length; i++) {
            resolvedGroupBy[i] = aliasMap.getOrDefault(groupByFields[i], groupByFields[i]);
        }

        Map<String, List<Table.Cell>> colMap = table.getAsMap();
        int rowCount = table.getRows().size();

        // Single-pass online aggregation. For each row we resolve its group key and update the
        // running aggregator for every (group, aggColumn) pair. Memory is O(G * C) rather than
        // O(N) for the row-index lists used by the previous two-pass design.
        Map<GroupKey, Aggregator[]> groups = new LinkedHashMap<>();
        for (int row = 0; row < rowCount; row++) {
            // Build key from group-by values (uses List.equals/hashCode — collision-free).
            List<Object> keyValues = new ArrayList<>(resolvedGroupBy.length);
            for (String gb : resolvedGroupBy) {
                List<Table.Cell> col = colMap.get(gb);
                keyValues.add(col != null ? col.get(row).value : null);
            }
            GroupKey key = new GroupKey(keyValues);
            Aggregator[] aggregators = groups.computeIfAbsent(key, k -> {
                Aggregator[] arr = new Aggregator[aggColumns.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = new Aggregator();
                }
                return arr;
            });
            for (int i = 0; i < aggColumns.size(); i++) {
                AggColumn agg = aggColumns.get(i);
                List<Table.Cell> col = colMap.get(agg.field);
                Object val = col != null ? col.get(row).value : null;
                aggregators[i].add(val);
            }
        }

        // Build the result table.
        Table result = new Table();
        result.startHeaders();
        for (int g = 0; g < resolvedGroupBy.length; g++) {
            Table.Cell origHeader = table.getHeaderMap().get(resolvedGroupBy[g]);
            result.addCell(groupByFields[g], copyHeaderAttrString(origHeader, groupByFields[g]));
        }
        for (AggColumn agg : aggColumns) {
            result.addCell(agg.displayName, "text-align:right;desc:" + agg.func + "(" + agg.field + ")");
        }
        result.endHeaders();

        for (Map.Entry<GroupKey, Aggregator[]> entry : groups.entrySet()) {
            result.startRow();
            for (Object val : entry.getKey().values) {
                result.addCell(val);
            }
            Aggregator[] aggregators = entry.getValue();
            for (int i = 0; i < aggColumns.size(); i++) {
                result.addCell(aggregators[i].getValue(aggColumns.get(i).func));
            }
            result.endRow();
        }
        return result;
    }

    /**
     * Reconstructs a safe attribute string for the result header that mirrors the original column's
     * metadata. Values containing the attribute delimiters ({@code ;} or {@code :}) are skipped to
     * avoid producing a string that cannot be parsed back correctly.
     */
    private static String copyHeaderAttrString(Table.Cell origHeader, String fallbackDesc) {
        if (origHeader == null || origHeader.attr == null || origHeader.attr.isEmpty()) {
            return "desc:" + fallbackDesc;
        }
        StringBuilder sb = new StringBuilder();
        boolean hasDesc = false;
        for (Map.Entry<String, String> e : origHeader.attr.entrySet()) {
            String v = e.getValue();
            if (v == null) continue;
            if (v.indexOf(';') >= 0 || v.indexOf(':') >= 0) continue; // skip un-encodable values
            if (sb.length() > 0) sb.append(';');
            sb.append(e.getKey()).append(':').append(v);
            if ("desc".equals(e.getKey())) hasDesc = true;
        }
        if (!hasDesc) {
            if (sb.length() > 0) sb.append(';');
            sb.append("desc:").append(fallbackDesc);
        }
        return sb.toString();
    }

    static final class AggColumn {
        final String func;
        final String field;
        final String displayName;

        AggColumn(String func, String field, String displayName) {
            this.func = func;
            this.field = field;
            this.displayName = displayName;
        }
    }

    /**
     * Hashable group key over the resolved group-by values. Uses {@link List#equals(Object)} /
     * {@link List#hashCode()} so the key is collision-free regardless of cell value contents
     * (the previous design used {@code String.join("\0", ...)} which could in theory collide).
     */
    static final class GroupKey {
        final List<Object> values;
        private final int hash;

        GroupKey(List<Object> values) {
            this.values = values;
            this.hash = values.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GroupKey)) return false;
            return values.equals(((GroupKey) o).values);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Per-(group, column) running aggregator. Maintains the sum, min, max, count of contributing
     * (non-null and numerically-parseable) values, and a {@code countAll} including null/unparseable
     * rows for the {@code count} function. The first non-null value seen is captured as a
     * {@code sampleValue} so the aggregated output can be wrapped back into its source type
     * (ByteSizeValue, TimeValue, etc.).
     */
    static final class Aggregator {
        long countAll = 0;
        long countContributing = 0;
        Double sum = null;
        Double min = null;
        Double max = null;
        Object sampleValue = null;

        void add(Object value) {
            countAll++;
            if (value != null) {
                if (sampleValue == null) sampleValue = value;
                Double num = parseAsDouble(value);
                if (num != null) {
                    if (sum == null) sum = 0.0;
                    sum += num;
                    if (min == null || num < min) min = num;
                    if (max == null || num > max) max = num;
                    countContributing++;
                }
            }
        }

        Object getValue(String function) {
            switch (function) {
                case "count":
                    return countAll;
                case "sum":
                    return sum == null ? null : formatValue(sum, sampleValue);
                case "min":
                    return min == null ? null : formatValue(min, sampleValue);
                case "max":
                    return max == null ? null : formatValue(max, sampleValue);
                case "avg":
                    // sum of non-nulls divided by count of non-nulls — null values do not pull the
                    // average toward zero (catSummary divided by countAll instead, which is wrong
                    // when any row contributes null for the aggregation column).
                    if (countContributing == 0 || sum == null) return null;
                    double avg = sum / countContributing;
                    // For non-type-preserving outputs we keep two decimal places (matches the
                    // legacy inline implementation in RestTable).
                    if (sampleValue instanceof ByteSizeValue || sampleValue instanceof SizeValue || sampleValue instanceof TimeValue) {
                        return formatValue(avg, sampleValue);
                    }
                    return formatValue(Math.round(avg * 100.0) / 100.0, sampleValue);
                default:
                    return null;
            }
        }
    }

    /** Parse a cell value into a double for aggregation. Returns null if the value isn't numeric. */
    static Double parseAsDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof ByteSizeValue) return (double) ((ByteSizeValue) value).getBytes();
        if (value instanceof TimeValue) return (double) ((TimeValue) value).millis();
        if (value instanceof SizeValue) return (double) ((SizeValue) value).singles();
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    /** Wrap an aggregated double back into the original source type so renderers preserve units. */
    static Object formatValue(double num, Object sampleValue) {
        if (sampleValue instanceof ByteSizeValue) return new ByteSizeValue(safeLong(num));
        if (sampleValue instanceof SizeValue) return new SizeValue(safeLong(num));
        if (sampleValue instanceof TimeValue) return new TimeValue(safeLong(num));
        if (sampleValue instanceof Long || sampleValue instanceof Integer || sampleValue instanceof Short || sampleValue instanceof Byte) {
            return safeLong(num);
        }
        if (sampleValue instanceof String) {
            if (num == Math.floor(num) && !Double.isInfinite(num)) {
                return String.valueOf(safeLong(num));
            }
            return String.format(Locale.ROOT, "%.2f", num);
        }
        // Default: return Long when integral (consistent with the original inline impl), else Double.
        if (num == Math.floor(num) && !Double.isInfinite(num)) return safeLong(num);
        return num;
    }

    /**
     * Convert an aggregated double to a long using round-half-up semantics, clamping to
     * {@link Long#MIN_VALUE}/{@link Long#MAX_VALUE} instead of silently overflowing. This matters
     * for sums of many large {@link ByteSizeValue}s (which can exceed {@code Long.MAX_VALUE} as a
     * double) and prevents {@code ByteSizeValue}/{@code SizeValue} from being constructed with a
     * wrapped negative value.
     */
    static long safeLong(double num) {
        if (Double.isNaN(num)) return 0L;
        if (num >= (double) Long.MAX_VALUE) return Long.MAX_VALUE;
        if (num <= (double) Long.MIN_VALUE) return Long.MIN_VALUE;
        return Math.round(num);
    }
}
