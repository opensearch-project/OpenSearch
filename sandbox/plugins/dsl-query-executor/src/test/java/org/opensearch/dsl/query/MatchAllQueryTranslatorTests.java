/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

public class MatchAllQueryTranslatorTests extends OpenSearchTestCase {

    public void testConvertsMatchAllToTrueLiteral() throws ConversionException {
        ConversionContext ctx = TestUtils.createContext();
        MatchAllQueryTranslator translator = new MatchAllQueryTranslator();

        RexNode result = translator.convert(QueryBuilders.matchAllQuery(), ctx);

        assertTrue(result instanceof RexLiteral);
        assertTrue(RexLiteral.booleanValue(result));
    }

    public void testReportsCorrectQueryType() {
        MatchAllQueryTranslator translator = new MatchAllQueryTranslator();
        assertEquals(MatchAllQueryBuilder.class, translator.getQueryType());
    }

    public void testBoostParameterRejectedWithConversionException() {
        ConversionContext ctx = TestUtils.createContext();
        MatchAllQueryTranslator translator = new MatchAllQueryTranslator();

        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.matchAllQuery().boost(1.5f), ctx)
        );
        assertTrue("Must mention 'boost', got: " + ex.getMessage(), ex.getMessage().contains("boost"));
    }

    public void testNameParameterRejectedWithConversionException() {
        ConversionContext ctx = TestUtils.createContext();
        MatchAllQueryTranslator translator = new MatchAllQueryTranslator();

        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.matchAllQuery().queryName("my_match_all"), ctx)
        );
        assertEquals("Match_all query parameter '_name' is not supported", ex.getMessage());
    }
}
