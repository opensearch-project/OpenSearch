/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.searchonly;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

public class SearchOnlyAction extends ActionType<AcknowledgedResponse> {
    public static final SearchOnlyAction INSTANCE = new SearchOnlyAction();
    public static final String NAME = "indices:admin/searchonly";

    private SearchOnlyAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
