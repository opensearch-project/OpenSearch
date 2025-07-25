/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

/**
 * Utility class for setting up query builder proto converters in tests.
 */
public class QueryBuilderProtoTestUtils {

    private QueryBuilderProtoTestUtils() {
        // Utility class, no instances
    }

    /**
     * Sets up the registry with all built-in converters for testing.
     * This method should be called in the setUp method of test classes
     * that use AbstractQueryBuilderProtoUtils.
     */
    public static void setupRegistry() {
        // Create a new registry
        QueryBuilderProtoConverterRegistry registry = new QueryBuilderProtoConverterRegistry();

        // Register all built-in converters
        registry.registerConverter(new MatchAllQueryBuilderProtoConverter());
        registry.registerConverter(new MatchNoneQueryBuilderProtoConverter());
        registry.registerConverter(new TermQueryBuilderProtoConverter());
        registry.registerConverter(new TermsQueryBuilderProtoConverter());

        // Set the registry in AbstractQueryBuilderProtoUtils
        AbstractQueryBuilderProtoUtils.setRegistry(registry);
    }
}
