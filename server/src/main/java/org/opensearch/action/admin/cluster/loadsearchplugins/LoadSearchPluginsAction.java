/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadsearchplugins;

import org.opensearch.action.ActionType;

/**
 * Action for dynamically loading search plugins
 *
 * @opensearch.internal
 */
public class LoadSearchPluginsAction extends ActionType<LoadSearchPluginsResponse> {

    public static final LoadSearchPluginsAction INSTANCE = new LoadSearchPluginsAction();
    public static final String NAME = "cluster:admin/search_plugins/load";

    private LoadSearchPluginsAction() {
        super(NAME, LoadSearchPluginsResponse::new);
    }
}
