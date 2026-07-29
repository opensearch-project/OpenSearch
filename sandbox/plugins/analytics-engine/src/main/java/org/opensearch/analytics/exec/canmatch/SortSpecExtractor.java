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

        boolean descending = primary.getDirection().isDescending();
        logger.debug("sort-spec: column={} descending={}", column, descending);
        return new SortSpec(column, descending);
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
