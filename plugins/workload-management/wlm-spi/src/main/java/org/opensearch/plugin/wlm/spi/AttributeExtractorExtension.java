/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.spi;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;

/**
 * Extension point for providing a custom {@link AttributeExtractor}.
 * Implementations of this interface allow plugins to contribute their own
 * logic for extracting attributes that can be used by the Workload Management framework.
 */
public interface AttributeExtractorExtension {

    /**
     * AttributeExtractor getter
     */
    AttributeExtractor<String> getAttributeExtractor();
}
