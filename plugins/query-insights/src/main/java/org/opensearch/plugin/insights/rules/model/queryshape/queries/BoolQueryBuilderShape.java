/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.queries;

import org.opensearch.plugin.insights.rules.model.queryshape.misc.QueryBuilderShape;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class BoolQueryBuilderShape extends QueryBuilderShape {
    List<? extends QueryBuilderShape> filterClause;
    List<? extends QueryBuilderShape> mustClause;
    List<? extends QueryBuilderShape> mustNotClause;
    List<? extends QueryBuilderShape> shouldClause;

    @Override
    public int hashCode() {
        // Sort the lists before calculating the hash code
        sortLists();
        return Objects.hash(filterClause, mustClause, mustNotClause, shouldClause);
    }

    private void sortLists() {
        if (filterClause != null) {
            Collections.sort(filterClause);
        }
        if (mustClause != null) {
            Collections.sort(mustClause);
        }
        if (mustNotClause != null) {
            Collections.sort(mustNotClause);
        }
        if (shouldClause != null) {
            Collections.sort(shouldClause);
        }
    }
}
