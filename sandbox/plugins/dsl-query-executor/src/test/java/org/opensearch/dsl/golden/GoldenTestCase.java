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
    private Map<String, String> indexMapping;
    private Map<String, Object> inputDsl;
    private String expectedRelNodePlan;
    private List<String> executionFieldNames;
    private List<List<Object>> executionRows;
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

    public String getExpectedRelNodePlan() {
        return expectedRelNodePlan;
    }

    public void setExpectedRelNodePlan(String expectedRelNodePlan) {
        this.expectedRelNodePlan = expectedRelNodePlan;
    }

    public List<String> getExecutionFieldNames() {
        return executionFieldNames;
    }

    public void setExecutionFieldNames(List<String> executionFieldNames) {
        this.executionFieldNames = executionFieldNames;
    }

    public List<List<Object>> getExecutionRows() {
        return executionRows;
    }

    public void setExecutionRows(List<List<Object>> executionRows) {
        this.executionRows = executionRows;
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
