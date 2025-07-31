/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A test-specific implementation of QueryBuilderProtoConverterRegistry that starts with no converters.
 * This class is used for testing scenarios where we need a clean registry without any built-in converters.
 */
public class EmptyQueryBuilderProtoConverterRegistry extends QueryBuilderProtoConverterRegistry {

    private static final Logger logger = LogManager.getLogger(EmptyQueryBuilderProtoConverterRegistry.class);

    /**
     * Creates a new empty registry with no converters.
     * This constructor calls the parent constructor but doesn't register any converters.
     */
    public EmptyQueryBuilderProtoConverterRegistry() {
        // The parent constructor will call registerBuiltInConverters() and loadExternalConverters(),
        // but we'll override those methods to do nothing
    }

    /**
     * Override the parent's registerBuiltInConverters method to do nothing.
     * This ensures no built-in converters are registered.
     */
    @Override
    protected void registerBuiltInConverters() {
        // Do nothing - we want an empty registry for testing
        logger.debug("Skipping registration of built-in converters for testing");
    }

    /**
     * Override the parent's loadExternalConverters method to do nothing.
     * This ensures no external converters are loaded.
     */
    @Override
    protected void loadExternalConverters() {
        // Do nothing - we want an empty registry for testing
        logger.debug("Skipping loading of external converters for testing");
    }
}
