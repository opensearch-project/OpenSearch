/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.query.QueryBuilderTranslatorProvider;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ServiceLoader;

public class DslQueryBuilderTranslatorProviderTests extends OpenSearchTestCase {

    public void testTranslatesThroughDslRegistry() {
        ConversionContext context = TestUtils.createContext();

        RexNode result = new DslQueryBuilderTranslatorProvider().translate(
            QueryBuilders.termQuery("name", "laptop"),
            context.getCluster(),
            context.getTable()
        );

        assertEquals(SqlKind.EQUALS, result.getKind());
    }

    public void testRegisteredAsAnalyticsExtension() {
        QueryBuilderTranslatorProvider provider = ServiceLoader.load(QueryBuilderTranslatorProvider.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .filter(DslQueryBuilderTranslatorProvider.class::isInstance)
            .findFirst()
            .orElse(null);

        assertNotNull(provider);
    }
}
