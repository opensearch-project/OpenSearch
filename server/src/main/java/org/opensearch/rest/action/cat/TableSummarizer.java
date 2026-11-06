/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.common.Table;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.SizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.rest.RestRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds a grouped + aggregated view of a {@link Table} for {@code _cat} APIs. The view is requested with
 * two explicit parameters:
 * <ul>
 *   <li>{@code group_by}: comma-separated columns to group by. Optional; without it the aggregates are
 *   computed over all rows and a single row is returned.</li>
 *   <li>{@code aggregate}: comma-separated {@code function(column)} entries, where function is one of
 *   {@code sum}, {@code count}, {@code avg}, {@code min}, {@code max}. Required whenever the view is requested.</li>
 * </ul>
 * For example {@code GET _cat/shards?group_by=index&aggregate=count(shard),sum(store)}.
 *
 * <p>The result has the group-by columns first, then the aggregates, each in the order given. Group-by columns are
 * named by their canonical column name and keep the original column's aliases. Aggregates are named
 * {@code function(canonical column)} with a lower-case function, so {@code SUM(d)} on a column {@code docs}
 * becomes {@code sum(docs)}; the spelling the user typed is kept as an alias. The {@code h}, {@code s} and
 * {@code limit} parameters then apply to this result like they do to any other table.
 *
 * <p>When neither parameter is present the table is not touched, so ordinary {@code _cat} requests behave
 * exactly as they did before this view existed.
 *
 * <p>Numeric types {@link Long}, {@link Integer}, {@link Short}, {@link Byte}, {@link ByteSizeValue},
 * {@link SizeValue}, {@link TimeValue} are preserved through aggregation so downstream rendering with
 * {@code bytes=}/{@code time=}/{@code size=} continues to work on aggregated values.
 *
 * <p>The {@code avg} function computes {@code sum / count_of_non_null_values}; rows whose value is
 * null for a given aggregation column do not contribute to either the numerator or denominator
 * for that column's average.
 *
 * @opensearch.internal
 */
public final class TableSummarizer {

    public static final String GROUP_BY_PARAM = "group_by";
    public static final String AGGREGATE_PARAM = "aggregate";

    /** Matches function syntax like {@code sum(docs.count)}, {@code count(shard)}. Case-insensitive so {@code SUM(docs)} also matches. */
    private static final Pattern AGG_FUNC_PATTERN = Pattern.compile("^(sum|count|avg|min|max)\\((.+)\\)$", Pattern.CASE_INSENSITIVE);

    private TableSummarizer() {}

    /**
     * Returns true if the request asks for a grouped or aggregated view, that is if either {@code group_by} or
     * {@code aggregate} is present. A request with only {@code group_by} is still "requested" so that
     * {@link #parse(RestRequest)} can reject it with a clear error instead of silently returning a plain listing.
     */
    public static boolean isRequested(RestRequest request) {
        return request.hasParam(GROUP_BY_PARAM) || request.hasParam(AGGREGATE_PARAM);
    }

    /**
     * Parses and validates the syntax of {@code group_by} / {@code aggregate}. Column names are not checked here
     * because the table is not known yet; {@link #summarize(Table, Spec)} does that.
     *
     * @return the parsed request, or {@code null} if neither parameter is present
     * @throws IllegalArgumentException if the parameters are malformed (this maps to a 400)
     */
    static Spec parse(RestRequest request) {
        if (isRequested(request) == false) {
            return null;
        }
        return parse(request.param(GROUP_BY_PARAM), request.param(AGGREGATE_PARAM));
    }

    static Spec parse(String groupByParam, String aggregateParam) {
        List<AggColumn> aggregates = new ArrayList<>();
        for (String token : splitTokens(aggregateParam)) {
            Matcher m = AGG_FUNC_PATTERN.matcher(token);
            String field = m.matches() ? m.group(2).trim() : "";
            if (field.isEmpty()) {
                throw new IllegalArgumentException(
                    "Invalid aggregate [" + token + "]. Expected function(column) where function is one of sum, count, avg, min, max"
                );
            }
            aggregates.add(new AggColumn(m.group(1).toLowerCase(Locale.ROOT), field, token));
        }
        if (aggregates.isEmpty()) {
            throw new IllegalArgumentException(
                "[aggregate] must list at least one function such as count(column), and is required when [group_by] is set"
            );
        }
        return new Spec(new ArrayList<>(new LinkedHashSet<>(splitTokens(groupByParam))), aggregates);
    }

    /** Splits a comma-separated parameter, trimming each token and skipping blank ones. */
    private static List<String> splitTokens(String param) {
        List<String> tokens = new ArrayList<>();
        for (String token : Strings.splitStringByCommaToArray(param)) {
            String t = token.trim();
            if (t.isEmpty() == false) {
                tokens.add(t);
            }
        }
        return tokens;
    }

    /**
     * Builds the grouped and aggregated table.
     *
     * @throws IllegalArgumentException if a group-by or aggregate column does not exist in the table
     */
    static Table summarize(Table table, Spec spec) {
        Map<String, String> aliasMap = table.getAliasMap();
        Map<String, List<Table.Cell>> colMap = table.getAsMap();

        // Resolve aliases to canonical names and validate. Duplicates (by canonical name) collapse into the first.
        Map<String, String> groupBy = new LinkedHashMap<>(); // canonical -> typed
        for (String token : spec.groupBy) {
            String canonical = aliasMap.getOrDefault(token, token);
            if (colMap.containsKey(canonical) == false) {
                throw new IllegalArgumentException("Unknown column in group_by: '" + token + "'");
            }
            groupBy.putIfAbsent(canonical, token);
        }
        Map<String, ResolvedAgg> aggregates = new LinkedHashMap<>(); // canonical name -> aggregate
        for (AggColumn agg : spec.aggregates) {
            String field = aliasMap.getOrDefault(agg.field, agg.field);
            if (colMap.containsKey(field) == false) {
                throw new IllegalArgumentException("Unknown column in aggregate " + agg.func + "(...): '" + agg.field + "'");
            }
            aggregates.computeIfAbsent(agg.func + "(" + field + ")", name -> new ResolvedAgg(name, agg.func, field)).typed.add(agg.typed);
        }

        List<String> groupByFields = new ArrayList<>(groupBy.keySet());
        List<ResolvedAgg> aggColumns = new ArrayList<>(aggregates.values());
        int rowCount = table.getRows().size();

        // Single-pass online aggregation. For each row we resolve its group key and update the
        // running aggregator for every (group, aggColumn) pair. Memory is O(G * C) rather than
        // O(N) for the row-index lists used by a two-pass design.
        Map<GroupKey, Aggregator[]> groups = new LinkedHashMap<>();
        for (int row = 0; row < rowCount; row++) {
            // Build key from group-by values (uses List.equals/hashCode: collision-free).
            List<Object> keyValues = new ArrayList<>(groupByFields.size());
            for (String gb : groupByFields) {
                keyValues.add(colMap.get(gb).get(row).value);
            }
            Aggregator[] aggregators = groups.computeIfAbsent(new GroupKey(keyValues), k -> {
                Aggregator[] arr = new Aggregator[aggColumns.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = new Aggregator();
                }
                return arr;
            });
            for (int i = 0; i < aggColumns.size(); i++) {
                aggregators[i].add(colMap.get(aggColumns.get(i).field).get(row).value);
            }
        }

        // Build the result table: group-by columns first, then aggregates, each in the order given.
        Table result = new Table();
        result.startHeaders();
        for (String gb : groupByFields) {
            result.addCell(gb, copyHeaderAttrString(table.getHeaderMap().get(gb), gb));
        }
        for (ResolvedAgg agg : aggColumns) {
            result.addCell(agg.name, aggregateHeaderAttrString(agg));
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
     * Rejects an {@code h} parameter that names a column which is not in the aggregated result. Without this check
     * the display code silently skips names it cannot find, so {@code group_by=index&aggregate=sum(docs)&h=index,shard}
     * would return a table without {@code shard} and no hint why. Wildcard patterns are not checked: they expand
     * against the aggregated columns like they do for any other table. The names are compared the same way the
     * display code does (exact column name or alias, no trimming).
     *
     * @param summarized the table returned by {@link #summarize(Table, Spec)}
     * @param hParam     the raw {@code h} parameter, may be null
     * @throws IllegalArgumentException if a name in {@code h} is not a column of the aggregated table
     */
    static void validateDisplayHeaders(Table summarized, String hParam) {
        if (hParam == null) {
            return;
        }
        Set<String> known = new LinkedHashSet<>();
        List<String> columns = new ArrayList<>();
        for (Table.Cell header : summarized.getHeaders()) {
            known.add(header.value.toString());
            columns.add(header.value.toString());
            String aliases = header.attr.get("alias");
            if (aliases != null) {
                known.addAll(List.of(Strings.splitStringByCommaToArray(aliases)));
            }
        }
        for (String token : Strings.splitStringByCommaToArray(hParam)) {
            if (Regex.isSimpleMatchPattern(token) == false && known.contains(token) == false) {
                throw new IllegalArgumentException(
                    "Column ["
                        + token
                        + "] in h is not in the aggregated result. When group_by or aggregate is used, h can only select from: "
                        + String.join(", ", columns)
                );
            }
        }
    }

    /**
     * Reconstructs a safe attribute string for the result header that mirrors the original column's
     * metadata. Values containing the attribute delimiters ({@code ;} or {@code :}) are sanitized to
     * avoid producing a string that cannot be parsed back correctly. {@code default} and {@code sibling} are
     * not copied: a column that is hidden by default in the full table is still part of an aggregated
     * result when the user asked to group by it, and the sibling columns are not part of the result.
     */
    // Package-private for direct unit testing (delimiter-in-value handling).
    static String copyHeaderAttrString(Table.Cell origHeader, String fallbackDesc) {
        if (origHeader == null || origHeader.attr == null || origHeader.attr.isEmpty()) {
            return "desc:" + fallbackDesc;
        }
        StringBuilder sb = new StringBuilder();
        boolean hasDesc = false;
        for (Map.Entry<String, String> e : origHeader.attr.entrySet()) {
            String v = e.getValue();
            if (v == null || "default".equals(e.getKey()) || "sibling".equals(e.getKey())) continue;
            // Attribute strings are parsed on ';' and ':', so delimiter chars inside a value would
            // otherwise mangle the parse. Replace them with spaces so the attribute survives the
            // round-trip (the desc text is slightly reformatted; alias/text-align are unaffected
            // because those values never contain these characters in practice).
            if (v.indexOf(';') >= 0 || v.indexOf(':') >= 0) {
                v = v.replace(';', ' ').replace(':', ' ');
            }
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

    /** Header attributes of an aggregate column: right aligned, described, and aliased by the spellings the user typed. */
    private static String aggregateHeaderAttrString(ResolvedAgg agg) {
        StringBuilder sb = new StringBuilder("text-align:right;desc:").append(agg.func).append('(').append(agg.field).append(')');
        List<String> aliases = new ArrayList<>();
        for (String typed : agg.typed) {
            if (typed.equals(agg.name) == false && typed.indexOf(';') < 0 && typed.indexOf(':') < 0 && typed.indexOf(',') < 0) {
                aliases.add(typed);
            }
        }
        if (aliases.isEmpty() == false) {
            sb.append(";alias:").append(String.join(",", aliases));
        }
        return sb.toString();
    }

    /** A syntax-validated group_by / aggregate request: typed (not yet resolved) column names. */
    static final class Spec {
        final List<String> groupBy;
        final List<AggColumn> aggregates;

        Spec(List<String> groupBy, List<AggColumn> aggregates) {
            this.groupBy = groupBy;
            this.aggregates = aggregates;
        }
    }

    /** An aggregate as typed by the user: lower-case function, column as typed, and the whole token as typed. */
    static final class AggColumn {
        final String func;
        final String field;
        final String typed;

        AggColumn(String func, String field, String typed) {
            this.func = func;
            this.field = field;
            this.typed = typed;
        }
    }

    /** An aggregate resolved against a table: canonical column and result column name, plus every spelling typed for it. */
    private static final class ResolvedAgg {
        final String name;
        final String func;
        final String field;
        final Set<String> typed = new LinkedHashSet<>();

        ResolvedAgg(String name, String func, String field) {
            this.name = name;
            this.func = func;
            this.field = field;
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
     * (non-null and numerically-parseable) values, and separate row-counters for {@code count()}
     * semantics: {@code countNonNull} for {@code count(field)} (matches SQL {@code COUNT(field)}),
     * and {@code countAll} which counts every row seen regardless of value nullness. The first
     * non-null value seen is captured as a {@code sampleValue} so the aggregated output can be
     * wrapped back into its source type (ByteSizeValue, TimeValue, etc.).
     */
    static final class Aggregator {
        long countAll = 0;
        long countNonNull = 0;
        long countContributing = 0;
        Double sum = null;
        Double min = null;
        Double max = null;
        Object sampleValue = null;

        void add(Object value) {
            countAll++;
            if (value != null) {
                countNonNull++;
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
                    // SQL semantics: COUNT(field) excludes null values. Use countAll if the caller
                    // wants row count regardless of null (not currently exposed).
                    return countNonNull;
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
