/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer;

import org.opensearch.search.SearchHit;

import java.io.IOException;

/**
 * Deserializer for {@link SearchHit} which can be implemented for different types of serde mechanisms.
 */
public interface SearchHitDeserializer<T> {

    SearchHit createSearchHit(T inputStream) throws IOException;

}
