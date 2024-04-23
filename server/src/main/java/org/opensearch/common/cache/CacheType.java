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
    private final String apiRepresentation;

    private static final Map<String, CacheType> representationsMap;
    static {
        Map<String, CacheType> reprs = new HashMap<>();
        for (CacheType cacheType : values()) {
            reprs.put(cacheType.apiRepresentation, cacheType);
        }
        representationsMap = Collections.unmodifiableMap(reprs);
    }

    CacheType(String settingPrefix, String representation) {
        this.settingPrefix = settingPrefix;
        this.apiRepresentation = representation;
    }

    public String getSettingPrefix() {
        return settingPrefix;
    }

    public String getApiRepresentation() {
        return apiRepresentation;
    }

    public static CacheType getByRepresentation(String representation) {
        CacheType result = representationsMap.get(representation);
        if (result == null) {
            throw new IllegalArgumentException("No CacheType with representation = " + representation);
        }
        return result;
    }

    public static Set<String> allRepresentations() {
        return representationsMap.keySet();
    }
}
