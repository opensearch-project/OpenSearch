/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.registerplugins;

import org.opensearch.action.ActionType;

/**
 * Action for registering analysis components from loaded plugins
 *
 * @opensearch.internal
 */
public class RegisterPluginsAction extends ActionType<RegisterPluginsResponse> {

    public static final RegisterPluginsAction INSTANCE = new RegisterPluginsAction();
    public static final String NAME = "cluster:admin/plugins/register";

    private RegisterPluginsAction() {
        super(NAME, RegisterPluginsResponse::new);
    }
}
