/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumMap;
import java.util.Map;

public class ScalarFunctionTests extends OpenSearchTestCase {

    /** Non-OTHER_FUNCTION SqlKinds must be unique: fromSqlKind picks the first match and would shadow later entries. */
    public void testNoDuplicateSqlKindBindings() {
        Map<SqlKind, ScalarFunction> claimedBy = new EnumMap<>(SqlKind.class);
        for (ScalarFunction func : ScalarFunction.values()) {
            SqlKind kind = func.getSqlKind();
            if (kind == SqlKind.OTHER_FUNCTION) {
                continue;
            }
            ScalarFunction existing = claimedBy.put(kind, func);
            if (existing != null) {
                fail("SqlKind." + kind + " claimed by both " + existing + " and " + func);
            }
        }
    }

    public void testSargPredicateIsBoundToSqlKindSearch() {
        assertSame(ScalarFunction.SARG_PREDICATE, ScalarFunction.fromSqlKind(SqlKind.SEARCH));
    }
}
