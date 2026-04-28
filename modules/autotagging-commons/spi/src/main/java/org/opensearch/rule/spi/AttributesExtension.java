/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.spi;

import org.opensearch.rule.autotagging.Attribute;

/**
 * Represents a custom attribute extension for the rule framework.
 * Implementations provide a single attribute to be used in auto tagging.
 */
public interface AttributesExtension {
    /**
     * Returns the attribute provided by this extension.
     */
    Attribute getAttribute();
}
