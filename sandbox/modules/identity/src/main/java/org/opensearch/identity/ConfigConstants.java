/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

public class ConfigConstants {
    public static final String IDENTITY_CONFIG_PREFIX = "_identity_";
    public static final String IDENTITY_CONF_REQUEST_HEADER = IDENTITY_CONFIG_PREFIX + "conf_request";
    public static final String IDENTITY_DEFAULT_CONFIG_INDEX = ".identity_config";
    public static final String IDENTITY_CONFIG_INDEX_NAME = "identity.config_index_name";
    public static final String IDENTITY_AUTH_MANAGER_CLASS = "identity.auth_manager_class";
    public static final String IDENTITY_ALLOW_DEFAULT_INIT_SECURITYINDEX = "plugins.identity.allow_default_init_securityindex";
    public static final String IDENTITY_ENABLED = "identity.enabled";

    // REST Action and API
    public static final String IDENTITY_REST_REQUEST_PREFIX = "_identity";
    public static final String IDENTITY_REST_API_REQUEST_PREFIX = IDENTITY_REST_REQUEST_PREFIX + "/api";
    public static final String IDENTITY_USER_ACTION_SUFFIX = "_user_action";
    public static final String IDENTITY_CREATE_USER_ACTION = "create" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_READ_USER_ACTION = "read" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_UPDATE_USER_ACTION = "update" + IDENTITY_USER_ACTION_SUFFIX;
    public static final String IDENTITY_DELETE_USER_ACTION = "delete" + IDENTITY_USER_ACTION_SUFFIX;

}
