/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest;

/**
 * Contains constants used in rest package
 */
public class IdentityRestConstants {
    // REST Action and API
    public static final String IDENTITY_REST_REQUEST_PREFIX = "_identity";
    public static final String IDENTITY_REST_API_REQUEST_PREFIX = IDENTITY_REST_REQUEST_PREFIX + "/api";

    // user actions to identify an action class
    public static final String IDENTITY_USER_ACTION_SUFFIX = "_user_action";
    public static final String IDENTITY_CREATE_USER_ACTION = "create" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_READ_USER_ACTION = "read" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_UPDATE_USER_ACTION = "update" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_DELETE_USER_ACTION = "delete" + IDENTITY_USER_ACTION_SUFFIX;
}
