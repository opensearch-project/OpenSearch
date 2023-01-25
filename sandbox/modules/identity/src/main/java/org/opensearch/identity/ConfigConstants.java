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
}
