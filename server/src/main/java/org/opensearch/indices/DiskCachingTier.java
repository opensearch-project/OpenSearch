/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

/**
 * This is specific to disk caching tier and can be used to add methods which are specific to disk tier.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface DiskCachingTier<K, V> extends CachingTier<K, V> {

}
