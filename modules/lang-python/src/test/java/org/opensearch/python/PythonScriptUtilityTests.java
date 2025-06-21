/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import org.opensearch.test.OpenSearchTestCase;

public class PythonScriptUtilityTests extends OpenSearchTestCase {

    public void testValidExpression() {
        String[] expressions =
                new String[] {
                    "3 + 5",
                    "print('Hello')",
                    "lambda x: x * 2",
                    "if x == 1: print(x)",
                    "doc['a'].value + 2",
                    "-_score * 3",
                    "abs(_score)",
                    "sum(doc[\"ratings\"]) / len(doc[\"ratings\"])"
                };
        Boolean[] expectations = new Boolean[] {true, true, false, false, true, true, true, true};
        Iterator<String> exprIt = Arrays.stream(expressions).iterator();
        Iterator<Boolean> expectIt = Arrays.stream(expectations).iterator();
        while (exprIt.hasNext() && expectIt.hasNext()) {
            String expr = exprIt.next();
            boolean expect = expectIt.next();
            assertEquals(
                    expr + (expect ? " should" : " should not") + " be an expression",
                    expect,
                    PythonScriptUtility.isCodeAnExpression(expr));
        }

        //        assertTrue("Basic arithmetic should be an expression",
        // PythonScriptUtility.isCodeAnExpression("3 + 5"));
    }

    public void testExtractAccessedFields() {
        String code = "doc['a'].value + doc['b'].value * 2 + book['c'].value";
        Set<String> expectedFields = Set.of("a", "b");

        Set<String> fields = PythonScriptUtility.extractAccessedDocFields(code);
        assertEquals(expectedFields, fields);
    }
}
