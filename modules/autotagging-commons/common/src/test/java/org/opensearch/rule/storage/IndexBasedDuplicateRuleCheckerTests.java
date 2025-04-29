/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.storage;

import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.rule.action.GetRuleRequestTests._ID_ONE;
import static org.opensearch.rule.action.GetRuleRequestTests.ruleOne;

public class IndexBasedDuplicateRuleCheckerTests extends OpenSearchTestCase {
    IndexBasedDuplicateRuleChecker checker = new IndexBasedDuplicateRuleChecker();

    public void testNoDuplicate() {
        Map<String, Rule> existingRules = Collections.emptyMap();
        Optional<String> result = checker.getDuplicateRuleId(ruleOne, existingRules);
        assertFalse(result.isPresent());
    }

    public void testDuplicateExists() {
        Map<String, Rule> existingRules = Map.of(_ID_ONE, ruleOne);
        Optional<String> result = checker.getDuplicateRuleId(ruleOne, existingRules);
        assertTrue(result.isPresent());
        assertEquals(result.get(), _ID_ONE);
    }
}
