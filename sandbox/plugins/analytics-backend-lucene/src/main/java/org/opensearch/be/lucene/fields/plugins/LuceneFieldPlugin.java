/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.plugins;

import org.opensearch.be.lucene.fields.LuceneField;

import java.util.Collections;
import java.util.Map;

/**
 * Plugin interface for registering custom Lucene field implementations.
 */
public interface LuceneFieldPlugin {

    /**
     * Returns the Lucene field implementations provided by this plugin.
     * @return map of field type names to LuceneField implementations
     */
    default Map<String, LuceneField> getLuceneFields() {
        return Collections.emptyMap();
    }
}
