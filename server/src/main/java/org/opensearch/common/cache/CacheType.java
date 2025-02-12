/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Cache types available within OpenSearch.
 */
@ExperimentalApi
public enum CacheType {
    INDICES_REQUEST_CACHE("indices.requests.cache", "request_cache");

    private final String settingPrefix;
    private final String value; // The value displayed for this cache type in stats API responses

    private static final Map<String, CacheType> valuesMap;
    static {
        Map<String, CacheType> values = new HashMap<>();
        for (CacheType cacheType : values()) {
            values.put(cacheType.value, cacheType);
        }
        valuesMap = Collections.unmodifiableMap(values);
    }

    CacheType(String settingPrefix, String representation) {
        this.settingPrefix = settingPrefix;
        this.value = representation;
    }

    public String getSettingPrefix() {
        return settingPrefix;
    }

    public String getValue() {
        return value;
    }

    public static CacheType getByValue(String value) {
        CacheType result = valuesMap.get(value);
        if (result == null) {
            throw new IllegalArgumentException("No CacheType with value = " + value);
        }
        return result;
    }

    public static Set<String> allValues() {
        return valuesMap.keySet();
    }
}
