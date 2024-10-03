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

import java.util.Set;

/**
 * Settings related to cache.
 */
@ExperimentalApi
public class CacheSettings {

    /**
     * Only includes values which is power of 2 as we use bitwise logic: (key AND (segmentCount -1)) to calculate
     * segmentNumber which works well only with such values.
     */
    public static final Set<Integer> VALID_SEGMENT_COUNT_VALUES = Set.of(1, 2, 4, 8, 16, 32, 64, 128, 256);

    /**
     * Exception message for invalid segment number.
     */
    public static final String INVALID_SEGMENT_NUMBER_EXCEPTION_MESSAGE = "Cache: %s segment count should be " + "power of two up-to 256";

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
