/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.common;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Context of the XContent serialization, controlling how metadata is written.
 */
@ExperimentalApi
public enum XContentContext {
    /** Custom metadata returned as part of API call */
    API,

    /** Custom metadata stored as part of the persistent cluster state */
    GATEWAY,

    /** Custom metadata stored as part of a snapshot */
    SNAPSHOT;

    /** Parameter key used in ToXContent.Params to pass the context mode */
    public static final String PARAM_KEY = "context_mode";
}
