/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Hold the Attribute names to avoid the duplication and consistency.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class AttributeNames {

    /**
     * Constructor
     */
    private AttributeNames() {

    }

    /**
     * HTTP Protocol Version
     */
    public static final String HTTP_PROTOCOL_VERSION = "http.version";

    /**
     * HTTP method
     */
    public static final String HTTP_METHOD = "http.method";

    /**
     * HTTP Request URI.
     */
    public static final String HTTP_URI = "http.uri";

    /**
     * Http Request Query Parameters.
     */
    public static final String HTTP_REQ_QUERY_PARAMS = "url.query";

    /**
     * Rest Request ID.
     */
    public static final String REST_REQ_ID = "rest.request_id";

    /**
     * Rest Request Raw Path.
     */
    public static final String REST_REQ_RAW_PATH = "rest.raw_path";

    /**
     * Trace key. To be used for on demand sampling.
     */
    public static final String TRACE = "trace";

    /**
     * Transport Service send request target host.
     */
    public static final String TRANSPORT_TARGET_HOST = "target_host";

    /**
     * Transport Service send request local host.
     */
    public static final String TRANSPORT_HOST = "host";

    /**
     * Action Name.
     */
    public static final String TRANSPORT_ACTION = "action";

    /**
     * Task id
     */
    public static final String TASK_ID = "task_id";

    /**
     * Parent task id
     */
    public static final String PARENT_TASK_ID = "parent_task_id";

    /**
     * Index Name
     */
    public static final String INDEX = "index";

    /**
     * Shard ID
     */
    public static final String SHARD_ID = "shard_id";

    /**
     * Number of request items in bulk request
     */
    public static final String BULK_REQUEST_ITEMS = "bulk_request_items";

    /**
     * Node ID
     */
    public static final String NODE_ID = "node_id";

    /**
     * Refresh Policy
     */
    public static final String REFRESH_POLICY = "refresh_policy";

    /**
     * Search Response Total Hits
     */
    public static final String TOTAL_HITS = "total_hits";

    /**
     * Number of Indices
     */
    public static final String NUM_INDICES = "num_indices";
}
