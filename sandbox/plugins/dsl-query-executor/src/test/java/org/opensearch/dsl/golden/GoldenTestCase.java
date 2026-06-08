/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import java.util.List;
import java.util.Map;

/**
 * POJO representing a single golden file test case.
 *
 * <p>Each golden file encodes a complete test scenario: the input DSL, expected
 * RelNode plan, simulated execution rows, and expected output DSL. The
 * {@code indexMapping} field allows schema construction without a live cluster.
 */
public class GoldenTestCase {

    private String testName;
    private String indexName;
    // TODO: Consider centralizing indexMapping as a shared template to avoid duplication across golden files
    private Map<String, String> indexMapping;
    private Map<String, Object> inputDsl;
    private List<String> expectedRelNodePlan;
    private List<String> mockResultFieldNames;
    private List<List<Object>> mockResultRows;
    private Map<String, Object> expectedOutputDsl;
    private String planType;

    public String getTestName() {
        return testName;
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Map<String, String> getIndexMapping() {
        return indexMapping;
    }

    public void setIndexMapping(Map<String, String> indexMapping) {
        this.indexMapping = indexMapping;
    }

    public Map<String, Object> getInputDsl() {
        return inputDsl;
    }

    public void setInputDsl(Map<String, Object> inputDsl) {
        this.inputDsl = inputDsl;
    }

    public List<String> getExpectedRelNodePlan() {
        return expectedRelNodePlan;
    }

    public void setExpectedRelNodePlan(List<String> expectedRelNodePlan) {
        this.expectedRelNodePlan = expectedRelNodePlan;
    }

    public List<String> getMockResultFieldNames() {
        return mockResultFieldNames;
    }

    public void setMockResultFieldNames(List<String> mockResultFieldNames) {
        this.mockResultFieldNames = mockResultFieldNames;
    }

    public List<List<Object>> getMockResultRows() {
        return mockResultRows;
    }

    public void setMockResultRows(List<List<Object>> mockResultRows) {
        this.mockResultRows = mockResultRows;
    }

    public Map<String, Object> getExpectedOutputDsl() {
        return expectedOutputDsl;
    }

    public void setExpectedOutputDsl(Map<String, Object> expectedOutputDsl) {
        this.expectedOutputDsl = expectedOutputDsl;
    }

    public String getPlanType() {
        return planType;
    }

    public void setPlanType(String planType) {
        this.planType = planType;
    }

    @Override
    public String toString() {
        return testName;
    }
}
