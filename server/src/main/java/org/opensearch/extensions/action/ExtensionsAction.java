/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionType;

public class ExtensionsAction extends ActionType<ExtensionsActionResponse> {
    public static final String NAME = "cluster:monitor/extension";
    public static final ExtensionsAction INSTANCE = new ExtensionsAction();

    public ExtensionsAction() {
        super(NAME, ExtensionsActionResponse::new);
    }
}
