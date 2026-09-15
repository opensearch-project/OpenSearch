/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchSort;

import java.util.List;

/**
 * Finds a shard fragment's primary sort column and direction.
 *
 * <p>The Sort node is already in the fragment: {@code OpenSearchSortPushdownRewriter}
 * copies the collated Sort below the exchange and {@code DAGBuilder} makes that subtree
 * the shard fragment. Nothing needs plumbing from the planner.
 *
 * <p>Because that pushed-down Sort only exists for non-aggregate {@code sort | head N}
 * (the rewriter excludes aggregate top-K, joins, and un-limited sorts), a non-null return
 * here is itself the eligibility check — there's no second predicate to keep in sync.
 *
 * @opensearch.internal
 */
public final class SortSpecExtractor {

    private static final Logger logger = LogManager.getLogger(SortSpecExtractor.class);

    private SortSpecExtractor() {}

    /** Returns the primary sort spec, or {@code null} if there's no collated Sort with a fetch. */
    public static SortSpec extract(RelNode fragment) {
        OpenSearchSort sort = findCollatedSortWithFetch(fragment);
        if (sort == null) {
            return null;
        }

        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        RelFieldCollation primary = collations.get(0);

        // Resolve against the INPUT row type — that's what the shard scan produces, so the
        // name matches what the data node can look up in the parquet schema.
        List<RelDataTypeField> fields = sort.getInput().getRowType().getFieldList();
        int index = primary.getFieldIndex();
        if (index < 0 || index >= fields.size()) {
            logger.debug("sort-spec: primary collation index {} out of range for input row type", index);
            return null;
        }
        String column = fields.get(index).getName();

        int limit = resolveLimit(sort);
        if (limit <= 0) {
            return null;
        }

        boolean descending = primary.getDirection().isDescending();
        logger.debug("sort-spec: column={} descending={} limit={}", column, descending, limit);
        return new SortSpec(column, descending, limit);
    }

    /**
     * Rows the coordinator must collect from this fragment: {@code offset + fetch}, or just
     * {@code fetch} when there's no offset. Same sum as
     * {@code OpenSearchSortPushdownRewriter.shardFetch} asks each shard for, and for the same
     * reason — the offset window is dropped on the coordinator, after ordering.
     *
     * <p>Returns {@code -1} when the value can't be established: a non-literal bound (which the
     * rewriter refuses too), a non-positive count, or a sum overflowing {@code int}. Callers treat
     * that as "no spec" — an unknown budget isn't one to gate on.
     */
    private static int resolveLimit(OpenSearchSort sort) {
        if ((sort.fetch instanceof RexLiteral) == false) {
            logger.debug("sort-spec: non-literal fetch, no limit available");
            return -1;
        }
        int fetch = RexLiteral.intValue(sort.fetch);
        if (fetch <= 0) {
            logger.debug("sort-spec: non-positive fetch {}", fetch);
            return -1;
        }
        if (sort.offset == null) {
            return fetch;
        }
        if ((sort.offset instanceof RexLiteral) == false) {
            logger.debug("sort-spec: non-literal offset, no limit available");
            return -1;
        }
        int offset = RexLiteral.intValue(sort.offset);
        if (offset < 0) {
            logger.debug("sort-spec: negative offset {}", offset);
            return -1;
        }
        long total = (long) offset + fetch;
        if (total > Integer.MAX_VALUE) {
            logger.debug("sort-spec: offset {} + fetch {} overflows int", offset, fetch);
            return -1;
        }
        return (int) total;
    }

    /**
     * Finds the bottom-most Sort having both a collation and a fetch.
     *
     * <p>Stops at any multi-input operator (join, union): the rows reaching a Sort above one
     * aren't this fragment's scan alone, so its ordering says nothing about a single shard.
     */
    private static OpenSearchSort findCollatedSortWithFetch(RelNode node) {
        if (node == null) {
            return null;
        }
        if (node instanceof OpenSearchSort sort && sort.getCollation().getFieldCollations().isEmpty() == false && sort.fetch != null) {
            return sort;
        }
        List<RelNode> inputs = node.getInputs();
        return inputs.size() == 1 ? findCollatedSortWithFetch(inputs.get(0)) : null;
    }

}
