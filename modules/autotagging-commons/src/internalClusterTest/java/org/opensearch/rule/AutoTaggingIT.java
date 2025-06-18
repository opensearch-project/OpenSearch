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
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AutoTaggingIT extends OpenSearchTestCase {

    private final FeatureType featureType = new TestFeatureType();
    private final RulePersistenceService persistenceService = new TestRulePersistenceService();

    public void testCreateRulePersistence() {
        Rule rule = buildRule("create-id", "create-rule", "logs-*");
        CreateRuleRequest request = new CreateRuleRequest(rule);
        TestActionListener<CreateRuleResponse> listener = new TestActionListener<>();

        persistenceService.createRule(request, listener);

        assertTrue(listener.successCalled);
        assertEquals("create-id", listener.result.getRule().getId());
    }

    public void testUpdateRulePersistence() {
        Rule rule = buildRule("update-id", "updated rule", "metrics-*");
        UpdateRuleRequest request = new UpdateRuleRequest(
            rule.getId(),
            rule.getDescription(),
            rule.getAttributeMap(),
            rule.getFeatureValue(),
            rule.getFeatureType()
        );
        TestActionListener<UpdateRuleResponse> listener = new TestActionListener<>();

        persistenceService.updateRule(request, listener);

        assertTrue(listener.successCalled);
        assertEquals("update-id", listener.result.getRule().getId());
        assertEquals("updated rule", listener.result.getRule().getDescription());
    }

    public void testGetRulePersistence() {
        GetRuleRequest request = new GetRuleRequest("get-id", Map.of(), null, featureType);

        TestActionListener<GetRuleResponse> listener = new TestActionListener<>();

        persistenceService.getRule(request, listener);

        assertTrue(listener.successCalled);
        assertNotNull(listener.result);
        assertEquals(0, listener.result.getRules().size());
    }

    public void testDeleteRulePersistence() {
        DeleteRuleRequest request = new DeleteRuleRequest("delete-id", featureType);
        TestActionListener<AcknowledgedResponse> listener = new TestActionListener<>();

        persistenceService.deleteRule(request, listener);

        assertTrue(listener.successCalled);
        assertTrue(listener.result.isAcknowledged());
    }

    public void testGetRuleWithAttributeFilters() {
        Attribute attr = featureType.getAttributeFromName("index_pattern");
        Map<Attribute, Set<String>> filters = Map.of(attr, Set.of("logs-*"));

        GetRuleRequest request = new GetRuleRequest("some-id", filters, null, featureType);
        TestActionListener<GetRuleResponse> listener = new TestActionListener<>();

        persistenceService.getRule(request, listener);

        assertTrue(listener.successCalled);
        assertNotNull(listener.result);
    }

    public void testUpdateRuleWithEmptyDescriptionShouldFail() {
        Rule rule = null;
        try {
            rule = buildRule("bad-id", "", "data-*");
            fail("Expected ValidationException due to empty description");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Rule description can't be null or empty"));
        }
    }

    public void testDeleteRuleWithNulls() {
        DeleteRuleRequest request = new DeleteRuleRequest(null, featureType);
        TestActionListener<AcknowledgedResponse> listener = new TestActionListener<>();

        persistenceService.deleteRule(request, listener);
        assertTrue(listener.successCalled);
    }

    public void testCreateThenUpdateThenGet() {
        Rule rule = buildRule("roundtrip-id", "initial", "index-*");

        CreateRuleRequest createRequest = new CreateRuleRequest(rule);
        TestActionListener<CreateRuleResponse> createListener = new TestActionListener<>();
        persistenceService.createRule(createRequest, createListener);
        assertTrue(createListener.successCalled);

        UpdateRuleRequest updateRequest = new UpdateRuleRequest(
            rule.getId(),
            "updated",
            rule.getAttributeMap(),
            rule.getFeatureValue(),
            rule.getFeatureType()
        );
        TestActionListener<UpdateRuleResponse> updateListener = new TestActionListener<>();
        persistenceService.updateRule(updateRequest, updateListener);
        assertTrue(updateListener.successCalled);
        assertEquals("updated", updateListener.result.getRule().getDescription());

        GetRuleRequest getRequest = new GetRuleRequest(rule.getId(), Map.of(), null, featureType);
        TestActionListener<GetRuleResponse> getListener = new TestActionListener<>();
        persistenceService.getRule(getRequest, getListener);
        assertTrue(getListener.successCalled);
    }

    public void testCreateRuleWithMultipleIndexPatterns() {
        Rule rule = Rule.builder()
            .id("multi-id")
            .description("multi-index")
            .featureType(featureType)
            .featureValue("groupB")
            .updatedAt("2025-06-17T00:00:00Z")
            .attributeMap(Map.of(featureType.getAttributeFromName("index_pattern"), Set.of("logs-*", "metrics-*")))
            .build();

        CreateRuleRequest request = new CreateRuleRequest(rule);
        TestActionListener<CreateRuleResponse> listener = new TestActionListener<>();
        persistenceService.createRule(request, listener);

        assertTrue(listener.successCalled);
        assertEquals("multi-id", listener.result.getRule().getId());
        assertEquals(2, listener.result.getRule().getAttributeMap().get(featureType.getAttributeFromName("index_pattern")).size());
    }

    public void testUpdateRuleOnlyDescription() {
        Rule original = buildRule("partial-update-id", "desc-v1", "logs-*");

        CreateRuleRequest createRequest = new CreateRuleRequest(original);
        TestActionListener<CreateRuleResponse> createListener = new TestActionListener<>();
        persistenceService.createRule(createRequest, createListener);
        assertTrue(createListener.successCalled);

        UpdateRuleRequest updateRequest = new UpdateRuleRequest(
            original.getId(),
            "desc-v2",
            original.getAttributeMap(),
            original.getFeatureValue(),
            original.getFeatureType()
        );
        TestActionListener<UpdateRuleResponse> updateListener = new TestActionListener<>();
        persistenceService.updateRule(updateRequest, updateListener);

        assertTrue(updateListener.successCalled);
        assertEquals("desc-v2", updateListener.result.getRule().getDescription());
    }

    public void testCreateRuleWithDuplicateAttributeValues() {
        Attribute attr = featureType.getAttributeFromName("index_pattern");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Rule.builder()
                .id("dup-id")
                .description("duplicates")
                .featureType(featureType)
                .featureValue("groupD")
                .updatedAt("2025-06-17T00:00:00Z")
                .attributeMap(Map.of(attr, Set.of("logs-*", "logs-*")))
                .build();
        });

        assertTrue(ex.getMessage().contains("duplicate element") || ex.getMessage().contains("duplicate"));
    }

    public void testGetRuleRequestInvalidInput() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            new GetRuleRequest("", Map.of(), "", featureType).validate();
        });
        assertTrue(ex.getMessage().contains("cannot be empty"));
    }

    private Rule buildRule(String id, String description, String pattern) {
        return Rule.builder()
            .id(id)
            .description(description)
            .featureType(featureType)
            .featureValue("groupA")
            .updatedAt("2025-06-17T00:00:00Z")
            .attributeMap(Map.of(featureType.getAttributeFromName("index_pattern"), Set.of(pattern)))
            .build();
    }

    public static class TestFeatureType implements FeatureType {
        private final Map<String, Attribute> attributes = Map.of("index_pattern", new IndexPatternAttribute());

        @Override
        public String getName() {
            return "workload_group";
        }

        @Override
        public Map<String, Attribute> getAllowedAttributesRegistry() {
            return attributes;
        }

        @Override
        public Attribute getAttributeFromName(String name) {
            return attributes.get(name);
        }
    }

    public static class IndexPatternAttribute implements Attribute {
        @Override
        public String getName() {
            return "index_pattern";
        }
    }

    public static class TestRulePersistenceService implements RulePersistenceService {
        @Override
        public void createRule(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
            listener.onResponse(new CreateRuleResponse(request.getRule()));
        }

        @Override
        public void deleteRule(DeleteRuleRequest request, ActionListener<AcknowledgedResponse> listener) {
            listener.onResponse(new AcknowledgedResponse(true));
        }

        @Override
        public void getRule(GetRuleRequest request, ActionListener<GetRuleResponse> listener) {
            listener.onResponse(new GetRuleResponse(List.of(), null)); // mock returns empty list
        }

        @Override
        public void updateRule(UpdateRuleRequest request, ActionListener<UpdateRuleResponse> listener) {
            Rule rule = Rule.builder()
                .id(request.getId())
                .description(request.getDescription())
                .attributeMap(request.getAttributeMap())
                .featureType(request.getFeatureType())
                .featureValue(request.getFeatureValue())
                .updatedAt("2025-06-17T00:00:00Z")
                .build();
            listener.onResponse(new UpdateRuleResponse(rule));
        }
    }

    public static class TestActionListener<T> implements ActionListener<T> {
        public boolean successCalled = false;
        public T result = null;

        @Override
        public void onResponse(T t) {
            successCalled = true;
            result = t;
        }

        @Override
        public void onFailure(Exception e) {
            fail("Unexpected failure: " + e.getMessage());
        }
    }
}
