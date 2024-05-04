/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model.queryshape.misc;

public abstract class QueryBuilderShape implements Comparable<QueryBuilderShape> {

    @Override
    public int compareTo(QueryBuilderShape other) {
        return this.getClass().getName().compareTo(other.getClass().getName());
    }
}
