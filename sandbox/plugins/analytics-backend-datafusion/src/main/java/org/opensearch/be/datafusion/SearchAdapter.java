/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Expands Calcite's {@code SEARCH(x, Sarg[...])} operator into its equivalent
 * comparison / boolean tree before the plan is serialized to substrait.
 *
 * <p>PPL's frontend (and Calcite's own optimizer passes) fold {@code IN}-lists,
 * {@code BETWEEN}, and unions of ranges into the compact {@code SEARCH} form.
 * {@code org.apache.calcite.util.Sarg} is a Calcite-internal literal value
 * type — the substrait wire has no representation for it, and DataFusion's
 * substrait consumer has no handler for the {@code SEARCH} operator. If we
 * let the SEARCH through, conversion fails with "Unable to convert call".
 *
 * <p>Calcite ships {@code RexUtil.expandSearch} which inverts this fold —
 * {@code SEARCH(x, Sarg[{1,2,3}])} becomes
 * {@code OR(=(x,1), =(x,2), =(x,3))} (or the equivalent {@code IN} call when
 * supported), and {@code SEARCH(x, Sarg[[1..10]])} becomes
 * {@code AND(>=(x,1), <=(x,10))}. The expansion is lossless — the original
 * fold is reversible by construction.
 *
 * <p>Null semantics: {@code Sarg.nullAs} (the unknown-value policy) is
 * preserved by {@code expandSearch} — queries that had {@code x IS NULL} as
 * part of the SEARCH carry that through as an explicit {@code IS NULL} disjunct
 * in the output.
 *
 * @opensearch.internal
 */
class SearchAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getKind() != SqlKind.SEARCH) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Second arg (`null` predicate list) is Calcite's way of saying "no
        // known narrowing predicates are available for further simplification"
        // — the expansion is purely syntactic.
        return RexUtil.expandSearch(rexBuilder, null, original);
    }
}
