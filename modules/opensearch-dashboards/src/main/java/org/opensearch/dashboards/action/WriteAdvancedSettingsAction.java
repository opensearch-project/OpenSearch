/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionType;

public class WriteAdvancedSettingsAction extends ActionType<AdvancedSettingsResponse> {

    public static final WriteAdvancedSettingsAction INSTANCE = new WriteAdvancedSettingsAction();
    public static final String NAME = "osd:admin/advanced_settings/write";

    private WriteAdvancedSettingsAction() {
        super(NAME, AdvancedSettingsResponse::new);
    }
}
