/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.DocumentMapperForType;

import java.util.function.Supplier;

/**
 * Provides configuration for indexing operations.
 */
@ExperimentalApi
public interface ConfigurationProvider {

    /**
     * Gets the document mapper supplier.
     * @return the document mapper supplier
     */
    Supplier<DocumentMapperForType> getDocumentMapperForTypeSupplier();

    /**
     * Gets the codec name.
     * @return the codec name
     */
    String getCodecName();
}
