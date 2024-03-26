/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase.highlight.serializer;

import org.opensearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;

/**
 * Serializer for {@link HighlightField} which can be implemented for different types of serialization.
 */
public interface HighlightFieldSerializer<T> {

    HighlightField createHighLightField(T inputStream) throws IOException;
}
