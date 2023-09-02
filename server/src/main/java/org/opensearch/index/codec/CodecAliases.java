/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;

import java.util.Set;

/**
 * This {@link CodecAliases} allows us to manage the settings with {@link Codec}.
 *
 * @opensearch.internal
 */
public interface CodecAliases {
    Set<String> aliases();
}
