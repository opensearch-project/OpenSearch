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

/** Expands Calcite's {@code SEARCH(field, Sarg[...])} fold so substrait/DataFusion can consume the predicate. */
class SargAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getKind() != SqlKind.SEARCH) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode expanded = RexUtil.expandSearch(rexBuilder, null, original);
        // Flatten the right-leaning AND chain that expandSearch produces (e.g.
        // multi-point exclusions): Filter.<init> asserts RexUtil.isFlat on the condition.
        return RexUtil.flatten(rexBuilder, expanded);
    }
}
