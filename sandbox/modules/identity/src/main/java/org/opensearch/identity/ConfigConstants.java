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
    public static final String IDENTITY_CONF_REQUEST_HEADER = IDENTITY_CONFIG_PREFIX+"conf_request";
    public static final String IDENTITY_DEFAULT_CONFIG_INDEX = ".identity_config";
    public static final String IDENTITY_CONFIG_INDEX_NAME = "identity.config_index_name";
    public static final String IDENTITY_DISABLE_ENVVAR_REPLACEMENT = "plugins.identity.disable_envvar_replacement";
    public static final String IDENTITY_UNSUPPORTED_ACCEPT_INVALID_CONFIG = "plugins.identity.unsupported.accept_invalid_config";
    public static final String IDENTITY_ALLOW_DEFAULT_INIT_SECURITYINDEX = "plugins.identity.allow_default_init_securityindex";
    public static final String IDENTITY_BACKGROUND_INIT_IF_SECURITYINDEX_NOT_EXIST = "plugins.identity.background_init_if_securityindex_not_exist";

    public static final String IDENTITY_ENABLED = "identity.enabled";
}
