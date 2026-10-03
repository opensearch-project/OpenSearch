/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.ActionType;

public class GetAdvancedSettingsAction extends ActionType<AdvancedSettingsResponse> {

    public static final GetAdvancedSettingsAction INSTANCE = new GetAdvancedSettingsAction();
    public static final String NAME = "osd:admin/advanced_settings/get";

    private GetAdvancedSettingsAction() {
        super(NAME, AdvancedSettingsResponse::new);
    }
}
