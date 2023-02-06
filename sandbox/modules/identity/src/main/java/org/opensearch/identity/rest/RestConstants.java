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
    public static final String IDENTITY_PREFIX = "_identity";
    public static final String IDENTITY_API_PREFIX = IDENTITY_PREFIX + "/api";
    public static final String IDENTITY_API_PERMISSION_PREFIX = IDENTITY_API_PREFIX + "/permissions";

    public static final String PERMISSION_SUBPATH = "/permissions";
    public static final String IDENTITY_PUT_PERMISSION_SUFFIX = "/put";
    public static final String PERMISSION_ACTION_PREFIX = "permission_action";

}
