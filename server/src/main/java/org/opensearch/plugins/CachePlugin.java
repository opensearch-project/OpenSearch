/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.store.StoreAwareCache;
import org.opensearch.common.cache.store.enums.CacheStoreType;

import java.util.Map;

/**
 * Plugin to extend cache related classes
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CachePlugin {

    Map<CacheStoreType, StoreAwareCache.Factory> getCacheStoreTypeMap();

    String getName();
}
