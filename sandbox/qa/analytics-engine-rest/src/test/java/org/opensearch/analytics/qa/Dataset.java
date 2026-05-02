/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

/**
 * Descriptor for a test dataset loaded from {@code resources/datasets/{name}/}.
 * <p>
 * A dataset consists of:
 * <ul>
 *   <li>{@code mapping.json} — index mapping and settings</li>
 *   <li>{@code bulk.json} — bulk-indexable documents (NDJSON)</li>
 *   <li>{@code {language}/q{N}.{ext}} — query files by language</li>
 *   <li>{@code {language}/expected/q{N}.json} — expected responses (optional)</li>
 * </ul>
 */
public final class Dataset {

    /** The dataset name, used as the directory under {@code resources/datasets/}. */
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

    /** Path to the expected response resource for the given language and query number. */
    public String expectedResponseResourcePath(String language, int queryNumber) {
        return "datasets/" + name + "/" + language + "/expected/q" + queryNumber + ".json";
    }
}
