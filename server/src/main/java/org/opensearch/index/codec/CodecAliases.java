/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Set;

/**
 * This {@link CodecAliases} to provide aliases for the {@link Codec}.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public interface CodecAliases {

    /**
     * Retrieves a set of aliases for an codec.
     *
     * @return A non-null set of alias strings. If no aliases are available, an empty set should be returned.
     */
    default Set<String> aliases() {
        return Set.of();
    }
}
