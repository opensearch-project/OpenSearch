/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.settings;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.settings.Setting;

import java.util.List;

/**
 * Settings related to cache.
 */
@ExperimentalApi
public class CacheSettings {

    public static final List<Integer> VALID_SEGMENT_NUMBER_LIST = List.of(1, 2, 4, 8, 16, 32, 64, 128, 256);

    /**
     * Used to store cache store name for desired cache types within OpenSearch.
     * Setting pattern: {cache_type}.store.name
     * Example: indices.request.cache.store.name
     */
    public static final Setting.AffixSetting<String> CACHE_TYPE_STORE_NAME = Setting.suffixKeySetting(
        "store.name",
        (key) -> Setting.simpleString(key, "", Setting.Property.NodeScope)
    );

    public static Setting<String> getConcreteStoreNameSettingForCacheType(CacheType cacheType) {
        return CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(cacheType.getSettingPrefix());
    }
}
