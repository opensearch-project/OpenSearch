/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.TIMESTAMP_ONE;

public class RulePersistenceServiceTests extends OpenSearchTestCase {

    /**
     * A persistence service whose getRule serves fixed pages keyed by search_after ("" for the first page), and records
     * the cursors it was asked for.
     */
    private static class PagedRulePersistenceService implements RulePersistenceService {
        private final Map<String, GetRuleResponse> pages;
        private final Map<String, Exception> failures;
        final List<String> requestedCursors = new ArrayList<>();

        PagedRulePersistenceService(Map<String, GetRuleResponse> pages, Map<String, Exception> failures) {
            this.pages = pages;
            this.failures = failures;
        }

        @Override
        public void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener) {
            String cursor = request.getSearchAfter() == null ? "" : request.getSearchAfter();
            requestedCursors.add(cursor);
            if (failures.containsKey(cursor)) {
                listener.onFailure(failures.get(cursor));
            } else {
                listener.onResponse(pages.get(cursor));
            }
        }

        @Override
        public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
            throw new UnsupportedOperationException();
        }
    }

    private static Rule rule(String id) {
        return Rule.builder()
            .id(id)
            .description("description " + id)
            .featureType(RuleTestUtils.MockRuleFeatureType.INSTANCE)
            .featureValue(FEATURE_VALUE_ONE)
            .attributeMap(ATTRIBUTE_MAP)
            .updatedAt(TIMESTAMP_ONE)
            .build();
    }

    /**
     * Runs getAllRules and returns its result; fails the test if the listener got a failure or was not called.
     */
    private static List<Rule> getAllRules(RulePersistenceService service) {
        AtomicReference<List<Rule>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.getAllRules(RuleTestUtils.MockRuleFeatureType.INSTANCE, ActionListener.wrap(result::set, failure::set));
        assertNull(failure.get());
        assertNotNull(result.get());
        return result.get();
    }

    public void testGetAllRulesFollowsEveryPage() {
        Rule r1 = rule("id1"), r2 = rule("id2"), r3 = rule("id3");
        PagedRulePersistenceService service = new PagedRulePersistenceService(
            Map.of("", new GetRuleResponse(List.of(r1, r2), "id2"), "id2", new GetRuleResponse(List.of(r3), null)),
            Map.of()
        );

        assertEquals(List.of(r1, r2, r3), getAllRules(service));
        assertEquals(List.of("", "id2"), service.requestedCursors);
    }

    public void testGetAllRulesHandlesEmptyLastPage() {
        Rule r1 = rule("id1"), r2 = rule("id2");
        PagedRulePersistenceService service = new PagedRulePersistenceService(
            Map.of("", new GetRuleResponse(List.of(r1, r2), "id2"), "id2", new GetRuleResponse(List.of(), null)),
            Map.of()
        );

        assertEquals(List.of(r1, r2), getAllRules(service));
        assertEquals(List.of("", "id2"), service.requestedCursors);
    }

    public void testGetAllRulesReturnsOnlyTheFailureWhenALaterPageFails() {
        RuntimeException pageFailure = new RuntimeException("search failed");
        PagedRulePersistenceService service = new PagedRulePersistenceService(
            Map.of("", new GetRuleResponse(List.of(rule("id1")), "id1")),
            Map.of("id1", pageFailure)
        );
        AtomicReference<List<Rule>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();

        service.getAllRules(RuleTestUtils.MockRuleFeatureType.INSTANCE, ActionListener.wrap(result::set, failure::set));

        assertNull(result.get());
        assertSame(pageFailure, failure.get());
    }

    public void testGetAllRulesFailsWhenCursorDoesNotAdvance() {
        // The second page adds a new rule but returns the same cursor again.
        PagedRulePersistenceService service = new PagedRulePersistenceService(
            Map.of("", new GetRuleResponse(List.of(rule("id1")), "c1"), "c1", new GetRuleResponse(List.of(rule("id2")), "c1")),
            Map.of()
        );

        assertNoProgressFailure(service);
        assertEquals(List.of("", "c1"), service.requestedCursors);
    }

    public void testGetAllRulesFailsWhenAPageAddsNoNewRules() {
        // The second page advances the cursor but only repeats a rule already collected.
        Rule r1 = rule("id1");
        PagedRulePersistenceService service = new PagedRulePersistenceService(
            Map.of("", new GetRuleResponse(List.of(r1), "c1"), "c1", new GetRuleResponse(List.of(r1), "c2")),
            Map.of()
        );

        assertNoProgressFailure(service);
        assertEquals(List.of("", "c1"), service.requestedCursors);
    }

    /**
     * Asserts getAllRules fails rather than returning the rules read so far, which would look like the rest were deleted.
     */
    private static void assertNoProgressFailure(RulePersistenceService service) {
        AtomicReference<List<Rule>> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        service.getAllRules(RuleTestUtils.MockRuleFeatureType.INSTANCE, ActionListener.wrap(result::set, failure::set));
        assertNull(result.get());
        assertTrue(failure.get() instanceof IllegalStateException);
        assertTrue(failure.get().getMessage(), failure.get().getMessage().contains("stopped making progress"));
    }
}
