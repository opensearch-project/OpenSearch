/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.feature_value_resolver.FeatureValueAggregator.AggregationResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CandidateFeatureValuesTests extends OpenSearchTestCase {

    public void testFlattenedValuesAndFirstOccurrence() {
        List<Set<String>> input = List.of(Set.of("A", "B"), Set.of("C"), Set.of("A", "D"));

        CandidateFeatureValues cfv = new CandidateFeatureValues(input);
        assertEquals(Set.of("A", "B", "C", "D"), cfv.getFlattenedValues());
        assertEquals(0, cfv.getFirstOccurrenceIndex("A"));
        assertEquals(1, cfv.getFirstOccurrenceIndex("C"));
        assertEquals(2, cfv.getFirstOccurrenceIndex("D"));
        assertEquals(Integer.MAX_VALUE, cfv.getFirstOccurrenceIndex("X"));
    }

    public void testMergeOr() {
        CandidateFeatureValues cfv1 = new CandidateFeatureValues(List.of(Set.of("A"), Set.of("B")));
        CandidateFeatureValues cfv2 = new CandidateFeatureValues(List.of(Set.of("C"), Set.of("D"), Set.of("E")));

        CandidateFeatureValues merged = cfv1.merge(cfv2, AttributeExtractor.CombinationStyle.OR);
        assertEquals(
            Set.of("A", "C"),
            merged.getFlattenedValues()
                .stream()
                .filter(v -> v.equals("A") || v.equals("C"))
                .collect(HashSet::new, HashSet::add, HashSet::addAll)
        );
        assertTrue(merged.getFlattenedValues().containsAll(Set.of("A", "B", "C", "D", "E")));
    }

    public void testMergeAnd() {
        CandidateFeatureValues cfv1 = new CandidateFeatureValues(List.of(Set.of("A", "B"), Set.of("C")));
        CandidateFeatureValues cfv2 = new CandidateFeatureValues(List.of(Set.of("B", "C"), Set.of("C", "D")));

        CandidateFeatureValues merged = cfv1.merge(cfv2, AttributeExtractor.CombinationStyle.AND);
        assertTrue(merged.getFlattenedValues().containsAll(Set.of("B", "C")));
        assertFalse(merged.getFlattenedValues().contains("A"));
        assertFalse(merged.getFlattenedValues().contains("D"));
    }

    public void testResolveTieImmediateWinner() {
        CandidateFeatureValues cfv1 = new CandidateFeatureValues(List.of(Set.of("A"), Set.of("B")));
        Set<String> candidates = Set.of("A", "B");
        AggregationResult result = new AggregationResult(List.of(cfv1), candidates);
        Optional<String> winner = result.resolveLabel();
        assertTrue(winner.isPresent());
        assertEquals("A", winner.get());
    }

    public void testResolveTieResolvedInSecondIteration() {
        CandidateFeatureValues cfv1 = new CandidateFeatureValues(List.of(Set.of("A", "B")));
        CandidateFeatureValues cfv2 = new CandidateFeatureValues(List.of(Set.of("B"), Set.of("A")));
        Set<String> candidates = Set.of("A", "B");
        AggregationResult result = new AggregationResult(List.of(cfv1, cfv2), candidates);
        Optional<String> winner = result.resolveLabel();
        assertTrue(winner.isPresent());
        assertEquals("B", winner.get());
    }

    public void testResolveTieNoWinner() {
        CandidateFeatureValues cfv1 = new CandidateFeatureValues(List.of(Set.of("A", "B")));
        CandidateFeatureValues cfv2 = new CandidateFeatureValues(List.of(Set.of("B", "A")));
        Set<String> candidates = Set.of("A", "B");
        AggregationResult result = new AggregationResult(List.of(cfv1, cfv2), candidates);
        Optional<String> winner = result.resolveLabel();
        assertTrue(winner.isEmpty());
    }

    public void testToStringContainsValues() {
        CandidateFeatureValues cfv = new CandidateFeatureValues(List.of(Set.of("A")));
        String str = cfv.toString();
        assertTrue(str.contains("A"));
        assertTrue(str.contains("values="));
    }
}
