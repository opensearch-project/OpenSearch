/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.spi;

import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.autotagging.FeatureType;

import java.util.function.Supplier;

/**
 * This interface exposes methods for the RuleFrameworkPlugin to extract framework related
 * implementations from the consumer plugins of this extension
 */
public interface RuleFrameworkExtension {
    /**
     * This method is used to flow implementation from consumer plugins into framework plugin
     * @return the plugin specific implementation of RulePersistenceService
     */
    Supplier<RulePersistenceService> getRulePersistenceServiceSupplier();

    /**
     * It tells the framework its FeatureType which can be used by Transport classes to handle the
     * consumer specific persistence
     * @return
     */
    FeatureType getFeatureType();
}
