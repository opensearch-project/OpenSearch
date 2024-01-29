/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.enums;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Cache store types in tiered cache.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum CacheStoreType {

    ON_HEAP,
    DISK
}
