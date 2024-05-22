/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer;

import org.opensearch.search.SearchHit.NestedIdentity;

import java.io.IOException;

/**
 * Deserializer for {@link NestedIdentity} which can be implemented for different types of serde mechanisms.
 */
public interface NestedIdentityDeserializer<T> {

    public NestedIdentity createNestedIdentity(T inputStream) throws IOException;
}
