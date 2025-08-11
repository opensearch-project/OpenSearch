/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.xcontent.XContentContext;

/**
 * Metadata Utilities.
 *
 * @opensearch.internal
 */
public class MetadataUtils {

    private MetadataUtils() {}

    public static final String CONTEXT_MODE_PARAM = "context_mode";

    public static final String SINGLE_MAPPING_NAME = "_doc";

    public static final String CONTEXT_MODE_GATEWAY = XContentContext.GATEWAY.toString();

    public static final String CONTEXT_MODE_API = XContentContext.API.toString();
}
