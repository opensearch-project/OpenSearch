/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest;

public class RestConstants {
    // REST Action and API
    public static final String IDENTITY_REST_REQUEST_PREFIX = "_identity";
    public static final String IDENTITY_REST_API_REQUEST_PREFIX = IDENTITY_REST_REQUEST_PREFIX + "/api";
    public static final String IDENTITY_USER_ACTION_SUFFIX = "_user_action";
    public static final String IDENTITY_PERMISSION_SUFFIX = IDENTITY_REST_API_REQUEST_PREFIX + "/permissions";
    public static final String IDENTITY_PUT_PERMISSION_SUFFIX = "/put";

}
