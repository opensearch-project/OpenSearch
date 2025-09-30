/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

public class MatchLabelTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        MatchLabel<String> label = new MatchLabel<>("value1", 0.85f);
        assertEquals("value1", label.getFeatureValue());
        assertEquals(0.85f, label.getMatchScore());
    }

    public void testDifferentType() {
        MatchLabel<Integer> label = new MatchLabel<>(123, 1.0f);
        assertEquals(Optional.of(123), label.getFeatureValue());
        assertEquals(1.0f, label.getMatchScore());
    }
}
