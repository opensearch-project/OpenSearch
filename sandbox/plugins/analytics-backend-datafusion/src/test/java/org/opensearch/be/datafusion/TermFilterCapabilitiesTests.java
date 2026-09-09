/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * DataFusion evaluates equality as whole-value comparison on the stored column, which is not the
 * Lucene term query on analyzed text. The plugin therefore must not declare exact-match operators
 * for the text family, so the planner routes them to the index-backed backend.
 */
public class TermFilterCapabilitiesTests extends OpenSearchTestCase {

    private static final Set<ScalarFunction> TERM_OPS = Set.of(ScalarFunction.EQUALS, ScalarFunction.NOT_EQUALS, ScalarFunction.IN);

    private static boolean declares(ScalarFunction function, FieldType fieldType) {
        DataFusionAnalyticsBackendPlugin backendPlugin = new DataFusionAnalyticsBackendPlugin(new DataFusionPlugin());
        for (FilterCapability cap : backendPlugin.getCapabilityProvider().filterCapabilities()) {
            if (cap instanceof FilterCapability.Standard standard
                && standard.function() == function
                && standard.fieldTypes().contains(fieldType)) {
                return true;
            }
        }
        return false;
    }

    public void testTermOpsNotDeclaredForText() {
        for (ScalarFunction op : TERM_OPS) {
            for (FieldType type : FieldType.text()) {
                assertFalse(op + " must not be declared for " + type, declares(op, type));
            }
        }
    }

    public void testTermOpsDeclaredForKeywordAndNumeric() {
        for (ScalarFunction op : TERM_OPS) {
            assertTrue(declares(op, FieldType.KEYWORD));
            assertTrue(declares(op, FieldType.INTEGER));
        }
    }

    public void testNonTermOpsStillDeclaredForText() {
        assertTrue(declares(ScalarFunction.IS_NULL, FieldType.TEXT));
        assertTrue(declares(ScalarFunction.LIKE, FieldType.TEXT));
    }
}
