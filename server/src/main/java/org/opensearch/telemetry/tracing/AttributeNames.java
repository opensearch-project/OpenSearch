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
     * Action Name.
     */
    public static final String TRANSPORT_ACTION = "action";
}
