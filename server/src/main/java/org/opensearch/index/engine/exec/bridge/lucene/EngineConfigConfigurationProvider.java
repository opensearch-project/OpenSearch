/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge.lucene;

import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.bridge.ConfigurationProvider;
import org.opensearch.index.mapper.DocumentMapperForType;

import java.util.function.Supplier;

/**
 * Configuration provider that wraps EngineConfig.
 */
public class EngineConfigConfigurationProvider implements ConfigurationProvider {

    private final EngineConfig engineConfig;

    /**
     * Creates a new configuration provider.
     * @param engineConfig the engine config to wrap
     */
    public EngineConfigConfigurationProvider(EngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    /**
     * Gets the document mapper supplier.
     * @return the document mapper supplier
     */
    @Override
    public Supplier<DocumentMapperForType> getDocumentMapperForTypeSupplier() {
        return engineConfig.getDocumentMapperForTypeSupplier();
    }

    /**
     * Gets the codec name.
     * @return the codec name
     */
    @Override
    public String getCodecName() {
        return engineConfig.getCodec().getName();
    }
}
