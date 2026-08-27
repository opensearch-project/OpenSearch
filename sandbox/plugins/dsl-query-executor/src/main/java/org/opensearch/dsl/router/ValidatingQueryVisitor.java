/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryTranslator;
import org.opensearch.dsl.query.ValidationResult;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

import java.util.List;

/**
 * Validates each query node the {@link QueryBuilder#visit} walk reaches: the node's type must have a
 * translator (else rejected), which validates that node's own parameters. {@link #getChildVisitor}
 * returns {@code this}, so one instance validates the whole tree; the first failing node wins.
 */
final class ValidatingQueryVisitor implements QueryBuilderVisitor {

    private final QueryRegistry queryRegistry;
    private final List<String> issues;
    private boolean failed = false;

    ValidatingQueryVisitor(QueryRegistry queryRegistry, List<String> issues) {
        this.queryRegistry = queryRegistry;
        this.issues = issues;
    }

    /** @return true if any visited node was unsupported. */
    boolean failed() {
        return failed;
    }

    @Override
    public void accept(QueryBuilder query) {
        if (failed) {
            return;
        }
        QueryTranslator translator = queryRegistry.get(query.getClass());
        if (translator == null) {
            reject("query:" + query.getName());
            return;
        }
        ValidationResult validationResult = translator.validate(query);
        if (validationResult.isAccepted() == false) {
            reject(validationResult.reasonCode());
        }
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        return this;
    }

    private void reject(String reason) {
        issues.add(reason);
        failed = true;
    }
}
