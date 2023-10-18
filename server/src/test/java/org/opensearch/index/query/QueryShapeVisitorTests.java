/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.test.OpenSearchTestCase;

import static org.junit.Assert.assertEquals;

final class QueryShapeVisitorTests extends OpenSearchTestCase {
    public void testQueryShapeVisitor() {
        QueryBuilder builder = new BoolQueryBuilder().must(new TermQueryBuilder("foo", "bar"))
            .filter(new ConstantScoreQueryBuilder(new RangeQueryBuilder("timestamp").from("12345677").to("2345678")))
            .should(
                new BoolQueryBuilder().must(new MatchQueryBuilder("text", "this is some text"))
                    .mustNot(new RegexpQueryBuilder("color", "red.*"))
            )
            .must(new TermsQueryBuilder("genre", "action", "drama", "romance"));
        QueryShapeVisitor shapeVisitor = new QueryShapeVisitor();
        builder.visit(shapeVisitor);
        assertEquals(
            "{\"type\":\"bool\",\"must\"[{\"type\":\"term\"},{\"type\":\"terms\"}],\"filter\"[{\"type\":\"constant_score\",\"filter\"[{\"type\":\"range\"}]}],\"should\"[{\"type\":\"bool\",\"must\"[{\"type\":\"match\"}],\"must_not\"[{\"type\":\"regexp\"}]}]}",
            shapeVisitor.toJson()
        );
    }
}
