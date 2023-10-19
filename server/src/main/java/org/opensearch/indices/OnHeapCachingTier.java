/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

/**
 * This is specific to onHeap caching tier and can be used to add methods which are specific to this tier.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface OnHeapCachingTier<K, V> extends CachingTier<K, V> {}
