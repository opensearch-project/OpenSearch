/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import org.opensearch.analytics.qa.Dataset;

import java.util.List;

/**
 * Describes one plan-shape workload: its dataset (mapping + bulk, via {@link Dataset}), where its
 * query files and golden files live, and which queries are exercised.
 *
 * <p>Onboarding a new workload (http_logs, big5, ...) is just constructing one of these plus its
 * {@code q{N}.plan.yaml} goldens — no harness changes.
 */
public final class WorkloadSpec {

    private final String name;
    private final Dataset dataset;
    private final String queryDir;
    private final String goldenDir;
    private final List<String> queryIds;

    public WorkloadSpec(String name, Dataset dataset, String queryDir, String goldenDir, List<String> queryIds) {
        this.name = name;
        this.dataset = dataset;
        this.queryDir = queryDir;
        this.goldenDir = goldenDir;
        this.queryIds = queryIds;
    }

    public String name() {
        return name;
    }

    /** Dataset descriptor used to provision the index (mapping + bulk). */
    public Dataset dataset() {
        return dataset;
    }

    /** Classpath dir holding query files referenced by a golden's {@code ppl_file}. */
    public String queryDir() {
        return queryDir;
    }

    /** Classpath dir holding the per-query {@code q{N}.plan.yaml} goldens. */
    public String goldenDir() {
        return goldenDir;
    }

    /** The query ids exercised by this workload (the allowlist). */
    public List<String> queryIds() {
        return queryIds;
    }

    /** Classpath path of a query's golden file. */
    public String goldenResourcePath(String queryId) {
        return goldenDir + "/" + queryId + ".plan.yaml";
    }
}
