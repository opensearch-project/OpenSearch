/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.ActionType;

/**
 * Transport action for updating ingestion state.
 *
 * @opensearch.experimental
 */
public class UpdateIngestionStateAction extends ActionType<UpdateIngestionStateResponse> {

    public static final UpdateIngestionStateAction INSTANCE = new UpdateIngestionStateAction();
    public static final String NAME = "indices:admin/ingestion/updateState";

    private UpdateIngestionStateAction() {
        super(NAME, UpdateIngestionStateResponse::new);
    }
}
