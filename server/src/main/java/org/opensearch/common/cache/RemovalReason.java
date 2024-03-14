/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.PublicApi;

/**
 * Reason for notification removal
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public enum RemovalReason {
    REPLACED,
    INVALIDATED,
    EVICTED,
    EXPLICIT,
    CAPACITY
}
