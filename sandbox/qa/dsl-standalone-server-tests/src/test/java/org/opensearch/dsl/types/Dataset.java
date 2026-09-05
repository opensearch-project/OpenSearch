/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

/**
 * Descriptor for a per-query-type test dataset loaded from {@code resources/datasets/{name}/}.
 * <p>
 * A dataset consists of:
 * <ul>
 *   <li>{@code mapping.json} — index mapping + settings (keeps the literal {@code "number_of_shards"}
 *       token so {@link DatasetProvisioner} can splice in the parquet/composite settings)</li>
 *   <li>{@code bulk.json} — bulk-indexable documents (NDJSON)</li>
 *   <li>{@code dsl/q{N}.json} — DSL query bodies (auto-discovered by {@link DatasetQueryRunner})</li>
 * </ul>
 * <p>
 * A local copy tailored to this standalone module — it does not depend on the analytics-engine-rest
 * test infrastructure (which drags in the test-ppl-frontend build).
 */
public final class Dataset {

    /** The dataset name == directory under {@code resources/datasets/}. */
    public final String name;

    /** The index name to provision the dataset into. */
    public final String indexName;

    public Dataset(String name, String indexName) {
        this.name = name;
        this.indexName = indexName;
    }

    /** Path to the mapping resource. */
    public String mappingResourcePath() {
        return "datasets/" + name + "/mapping.json";
    }

    /** Path to the bulk data resource. */
    public String bulkResourcePath() {
        return "datasets/" + name + "/bulk.json";
    }

    /** Path to a query resource for the given language and query number. */
    public String queryResourcePath(String language, String extension, int queryNumber) {
        return "datasets/" + name + "/" + language + "/q" + queryNumber + "." + extension;
    }

    /** Path to the golden expected-response resource for the given language and query number. */
    public String expectedResponseResourcePath(String language, int queryNumber) {
        return "datasets/" + name + "/" + language + "/expected/q" + queryNumber + ".json";
    }
}
