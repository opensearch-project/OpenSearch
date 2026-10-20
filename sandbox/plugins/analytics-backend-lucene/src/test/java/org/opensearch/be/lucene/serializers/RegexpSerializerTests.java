/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RegexpQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class RegexpSerializerTests extends OpenSearchTestCase {

    private static final SqlFunction REGEXP_FN = new SqlFunction(
        "REGEXP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final RegexpSerializer serializer = new RegexpSerializer();
    private RelDataTypeFactory typeFactory;
    private RexBuilder rexBuilder;
    private RelDataType varchar;

    private static final List<FieldStorageInfo> FIELD_STORAGE = List.of(
        new FieldStorageInfo("url", "keyword", FieldType.KEYWORD, List.of(), List.of("lucene"), List.of(), false)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        rexBuilder = new RexBuilder(typeFactory);
        varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    private RexNode regexpCall(String pattern) {
        return rexBuilder.makeCall(REGEXP_FN, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral(pattern));
    }

    public void testRegexpProducesRegexpQuery() {
        QueryBuilder qb = serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) regexpCall("google\\.com"), FIELD_STORAGE);
        assertTrue(qb instanceof RegexpQueryBuilder);
        RegexpQueryBuilder rq = (RegexpQueryBuilder) qb;
        assertEquals("url", rq.fieldName());
        assertEquals(".*google\\.com.*", rq.value());
    }

    public void testRegexpRoutesToExactMatchSubfield() {
        List<FieldStorageInfo> withSubfield = List.of(
            new FieldStorageInfo("msg", "text", FieldType.TEXT, List.of(), List.of("lucene"), List.of(), false, "keyword")
        );
        QueryBuilder qb = serializer.buildQueryBuilder((org.apache.calcite.rex.RexCall) regexpCall("error[0-9]+"), withSubfield);
        RegexpQueryBuilder rq = (RegexpQueryBuilder) qb;
        assertEquals("msg.keyword", rq.fieldName());
        assertEquals(".*error[0-9]+.*", rq.value());
    }
}
