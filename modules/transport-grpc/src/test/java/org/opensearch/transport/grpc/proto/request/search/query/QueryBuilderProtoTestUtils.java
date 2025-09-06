/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

/**
 * Utility class for setting up query builder proto converters in tests.
 */
public class QueryBuilderProtoTestUtils {

    private QueryBuilderProtoTestUtils() {
        // Utility class, no instances
    }

    /**
     * Creates an AbstractQueryBuilderProtoUtils instance with all built-in converters for testing.
     * This method should be called in test methods that need query parsing functionality.
     *
     * @return AbstractQueryBuilderProtoUtils instance configured for testing
     */
    public static AbstractQueryBuilderProtoUtils createQueryUtils() {
        // Create a new registry
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();

        // Register all built-in converters
        registry.registerConverter(new MatchAllQueryBuilderProtoConverter());
        registry.registerConverter(new MatchNoneQueryBuilderProtoConverter());
        registry.registerConverter(new TermQueryBuilderProtoConverter());
        registry.registerConverter(new TermsQueryBuilderProtoConverter());

        // Return an instance with the configured registry
        return new AbstractQueryBuilderProtoUtils(registry);
    }

    /**
     * Sets up the registry with all built-in converters for testing.
     * This method should be called in the setUp method of test classes
     * that use AbstractQueryBuilderProtoUtils.
     *
     * @deprecated Use createQueryUtils() for instance-based approach instead
     */
    @Deprecated
    public static void setupRegistry() {
        // This method is no longer used in the instance-based approach
        // Kept for any remaining legacy test compatibility
    }
}
