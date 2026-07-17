/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadplugins;

import org.opensearch.action.ActionType;

/**
 * Action for dynamically loading plugins
 *
 * @opensearch.internal
 */
public class LoadPluginsAction extends ActionType<LoadPluginsResponse> {

    public static final LoadPluginsAction INSTANCE = new LoadPluginsAction();
    public static final String NAME = "cluster:admin/plugins/load";

    private LoadPluginsAction() {
        super(NAME, LoadPluginsResponse::new);
    }
}
