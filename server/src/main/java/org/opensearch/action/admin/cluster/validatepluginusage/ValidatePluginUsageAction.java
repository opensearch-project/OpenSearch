/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.validatepluginusage;

import org.opensearch.action.ActionType;

/**
 * Action for validating plugin usage before uninstallation
 *
 * @opensearch.internal
 */
public class ValidatePluginUsageAction extends ActionType<ValidatePluginUsageResponse> {

    public static final ValidatePluginUsageAction INSTANCE = new ValidatePluginUsageAction();
    public static final String NAME = "cluster:admin/plugins/validate_usage";

    private ValidatePluginUsageAction() {
        super(NAME, ValidatePluginUsageResponse::new);
    }
}
