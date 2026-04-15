/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.removeplugins;

import org.opensearch.action.ActionType;

/**
 * Action for dynamically removing plugins
 *
 * @opensearch.internal
 */
public class RemovePluginsAction extends ActionType<RemovePluginsResponse> {

    public static final RemovePluginsAction INSTANCE = new RemovePluginsAction();
    public static final String NAME = "cluster:admin/plugins/remove";

    private RemovePluginsAction() {
        super(NAME, RemovePluginsResponse::new);
    }
}
