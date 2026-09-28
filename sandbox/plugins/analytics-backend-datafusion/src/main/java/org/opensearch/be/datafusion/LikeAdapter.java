/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Drops the 3rd escape operand from LIKE/ILIKE calls so Isthmus can serialize them via the
 * 2-arg {@code like} / {@code ilike} Substrait signatures. Calcite's grammar always emits
 * {@code LIKE(value, pattern, escape)} — the escape is almost always the default {@code '\'}
 * and is not expressible in either signature.
 *
 * <p>Case-insensitive semantics are preserved: PPL's default {@code like} lowers to
 * {@link org.apache.calcite.sql.fun.SqlLibraryOperators#ILIKE}, which {@link DataFusionFragmentConvertor}
 * maps to the custom {@code ilike} extension declared in {@code opensearch_scalar_functions.yaml}.
 * The adapter therefore leaves the operator untouched and only normalizes arity.
 *
 * @opensearch.internal
 */
class LikeAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 3) {
            return original;
        }
        return original.clone(original.getType(), original.getOperands().subList(0, 2));
    }
}
