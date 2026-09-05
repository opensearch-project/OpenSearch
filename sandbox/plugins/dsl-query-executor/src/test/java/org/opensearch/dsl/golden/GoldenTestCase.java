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
 *
 * <p><b>Harness limitation:</b> the response driver supplies mock rows to only the FIRST
 * matching plan of the declared type. Fixtures whose input DSL produces multiple aggregation
 * plans (e.g. sibling filter aggs, or a bucket sub-agg under a filter) must omit
 * {@code expectedOutputDsl} — they prove plan shape only, not counting correctness.
 * Use hand-authored multi-plan tests for sub-aggregation scoping coverage.
 *
 * <p><b>Explicit opt-out ({@code planShapeOnly}):</b> when a fixture legitimately cannot
 * provide {@code expectedOutputDsl} (because its multi-plan structure prevents a single
 * mock-row array from covering all plans), it MUST set {@code "planShapeOnly": true} in the
 * JSON. The loader rejects any fixture that omits {@code expectedOutputDsl} without this
 * flag — this prevents accidental loss of response assertion coverage. Conversely, setting
 * the flag while also providing {@code expectedOutputDsl} is contradictory and also rejected.
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
    private Map<String, Object> mockCountRow;
    private Map<String, Object> expectedOutputDsl;
    private String planType;
    private boolean planShapeOnly;

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

    /**
     * Column name → value for the request's COUNT plan single result row
     * (e.g. {@code {"_total": 5, "_notnull$brand": 5}}). Null when the scenario expects no
     * COUNT plan.
     */
    public Map<String, Object> getMockCountRow() {
        return mockCountRow;
    }

    public void setMockCountRow(Map<String, Object> mockCountRow) {
        this.mockCountRow = mockCountRow;
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

    /**
     * Returns {@code true} when this fixture intentionally omits {@code expectedOutputDsl}
     * because its multi-plan structure cannot be exercised by the single-plan response driver.
     * The loader rejects any fixture that omits {@code expectedOutputDsl} without setting this
     * flag, preventing silent loss of response assertion coverage.
     */
    public boolean isPlanShapeOnly() {
        return planShapeOnly;
    }

    public void setPlanShapeOnly(boolean planShapeOnly) {
        this.planShapeOnly = planShapeOnly;
    }

    @Override
    public String toString() {
        return testName;
    }
}
