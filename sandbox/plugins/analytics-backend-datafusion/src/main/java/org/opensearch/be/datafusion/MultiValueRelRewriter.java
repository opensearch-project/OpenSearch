/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;

/** Adds implicit element expansion for LIST-valued GROUP BY keys. */
final class MultiValueRelRewriter {

    private MultiValueRelRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                if (!(visited instanceof Aggregate aggregate)) {
                    return visited;
                }
                RelNode input = aggregate.getInput();
                boolean changed = false;
                for (int fieldIndex : aggregate.getGroupSet()) {
                    if (input.getRowType().getFieldList().get(fieldIndex).getType().getComponentType() != null) {
                        input = new MultiValueExpandRel(input, fieldIndex);
                        changed = true;
                    }
                }
                return changed
                    ? aggregate.copy(
                        aggregate.getTraitSet(),
                        input,
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        aggregate.getAggCallList()
                    )
                    : aggregate;
            }
        });
    }
}
