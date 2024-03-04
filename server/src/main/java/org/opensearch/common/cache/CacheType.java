/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Cache types available within OpenSearch.
 */
@ExperimentalApi
public enum CacheType {
    INDICES_REQUEST_CACHE("indices.requests.cache");

    private final String settingPrefix;

    CacheType(String settingPrefix) {
        this.settingPrefix = settingPrefix;
    }

    public String getSettingPrefix() {
        return settingPrefix;
    }
}
