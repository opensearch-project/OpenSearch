/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexLongFieldRange;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Coordinator-level utility that prunes shards based on per-index {@code @timestamp} range metadata
 * stored in {@link IndexMetadata}. This avoids sending network {@code can_match} probes to shards
 * whose index provably cannot contain data within the query's time range.
 *
 * @opensearch.experimental
 */
final class CoordinatorTimestampPruner {

    static final String TIMESTAMP_FIELD = "@timestamp";

    private CoordinatorTimestampPruner() {}

    /**
     * Examines the query for a {@code @timestamp} range filter and marks shards as skippable
     * when their index's timestamp range metadata indicates the query cannot match.
     *
     * @param shardsIts    the shard iterators to potentially prune
     * @param clusterState the current cluster state (provides IndexMetadata)
     * @param request      the search request (provides the query)
     */
    static void pruneShards(
        GroupShardsIterator<SearchShardIterator> shardsIts,
        ClusterState clusterState,
        SearchRequest request
    ) {
        SearchSourceBuilder source = request.source();
        if (source == null || source.query() == null) {
            return;
        }

        TimestampBounds bounds = extractTimestampBounds(source.query());
        if (bounds == null) {
            return;
        }

        LongSupplier nowSupplier = System::currentTimeMillis;
        long queryMin = resolveToEpochMillis(bounds.from, bounds.format, nowSupplier, false);
        long queryMax = resolveToEpochMillis(bounds.to, bounds.format, nowSupplier, true);

        if (queryMin == Long.MIN_VALUE && queryMax == Long.MAX_VALUE) {
            return;
        }

        for (SearchShardIterator shardIt : shardsIts) {
            if (shardIt.skip()) {
                continue;
            }

            IndexMetadata indexMetadata = clusterState.metadata().index(shardIt.shardId().getIndex());
            if (indexMetadata == null) {
                continue;
            }

            IndexLongFieldRange timestampRange = indexMetadata.getTimestampRange();
            IndexLongFieldRange.Relation relation = timestampRange.relation(queryMin, queryMax);

            if (relation == IndexLongFieldRange.Relation.DISJOINT) {
                shardIt.resetAndSkip();
            }
        }
    }

    /**
     * Extracts the tightest {@code @timestamp} range bounds from the query tree.
     * Looks for {@link RangeQueryBuilder} targeting {@code @timestamp} at the top level
     * or inside a {@link BoolQueryBuilder}'s {@code filter} or {@code must} clauses.
     */
    static TimestampBounds extractTimestampBounds(QueryBuilder query) {
        List<RangeQueryBuilder> candidates = new ArrayList<>();
        collectTimestampRanges(query, candidates);

        if (candidates.isEmpty()) {
            return null;
        }

        // If multiple ranges exist (e.g., in a bool filter), intersect them
        Object from = null;
        Object to = null;
        String format = null;
        for (RangeQueryBuilder range : candidates) {
            if (range.from() != null) {
                from = from == null ? range.from() : from; // take first non-null
            }
            if (range.to() != null) {
                to = to == null ? range.to() : to;
            }
            if (range.format() != null && format == null) {
                format = range.format();
            }
        }

        if (from == null && to == null) {
            return null;
        }

        return new TimestampBounds(from, to, format);
    }

    private static void collectTimestampRanges(QueryBuilder query, List<RangeQueryBuilder> candidates) {
        if (query instanceof RangeQueryBuilder) {
            RangeQueryBuilder range = (RangeQueryBuilder) query;
            if (TIMESTAMP_FIELD.equals(range.fieldName())) {
                candidates.add(range);
            }
        } else if (query instanceof BoolQueryBuilder) {
            BoolQueryBuilder bool = (BoolQueryBuilder) query;
            for (QueryBuilder clause : bool.filter()) {
                collectTimestampRanges(clause, candidates);
            }
            for (QueryBuilder clause : bool.must()) {
                collectTimestampRanges(clause, candidates);
            }
        }
    }

    /**
     * Resolves a date value (which may be a date math expression like "now-7d" or a formatted date string)
     * to epoch milliseconds.
     */
    static long resolveToEpochMillis(Object value, String format, LongSupplier nowSupplier, boolean roundUp) {
        if (value == null) {
            return roundUp ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        String dateStr = value.toString();
        DateFormatter formatter = format != null
            ? DateFormatter.forPattern(format)
            : DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        DateMathParser parser = formatter.toDateMathParser();

        Instant instant = parser.parse(dateStr, nowSupplier, roundUp, ZoneOffset.UTC);
        return instant.toEpochMilli();
    }

    /**
     * Holds the extracted bounds from a {@code @timestamp} range query.
     */
    static class TimestampBounds {
        final Object from;
        final Object to;
        final String format;

        TimestampBounds(Object from, Object to, String format) {
            this.from = from;
            this.to = to;
            this.format = format;
        }
    }
}
