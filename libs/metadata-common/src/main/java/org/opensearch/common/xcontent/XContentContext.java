/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

/**
 * Context of the XContent.
 * TODO: Remove this once {@code Metadata} is moved to this lib.
 */
public enum XContentContext {
    /* Custom metadata should be returns as part of API call */
    API,

    /* Custom metadata should be stored as part of the persistent cluster state */
    GATEWAY,

    /* Custom metadata should be stored as part of a snapshot */
    SNAPSHOT
}
