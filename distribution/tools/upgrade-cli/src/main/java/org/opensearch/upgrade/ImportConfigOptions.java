/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import java.nio.file.Path;

final class ImportConfigOptions {
    static final String OPENSEARCH_KEYSTORE_FILENAME = "opensearch.keystore";
    static final String OPENSEARCH_CONFIG_FILENAME = "opensearch.yml";
    static final String ES_CONFIG_FILENAME = "elasticsearch.yml";
    static final String ES_KEYSTORE_FILENAME = "elasticsearch.keystore";

    private final Path openSearchConfig;
    private final Path esConfig;

    ImportConfigOptions(final Path openSearchConfig, final Path esConfig) {
        this.openSearchConfig = openSearchConfig;
        this.esConfig = esConfig;
    }

    public Path getOpenSearchConfig() {
        return openSearchConfig;
    }

    public Path getOpenSearchYmlConfig() {
        return openSearchConfig.resolve(OPENSEARCH_CONFIG_FILENAME);
    }

    public Path getOpenSearchKeystore() {
        return openSearchConfig.resolve(OPENSEARCH_KEYSTORE_FILENAME);
    }

    public Path getEsConfig() {
        return esConfig;
    }

    public Path getESYmlConfig() {
        return esConfig.resolve(ES_CONFIG_FILENAME);
    }
}
