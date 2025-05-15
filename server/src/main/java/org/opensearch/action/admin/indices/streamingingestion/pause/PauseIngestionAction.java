/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.pause;

import org.opensearch.action.ActionType;

/**
 * Transport action for pausing ingestion.
 *
 * @opensearch.experimental
 */
public class PauseIngestionAction extends ActionType<PauseIngestionResponse> {

    public static final PauseIngestionAction INSTANCE = new PauseIngestionAction();
    public static final String NAME = "indices:admin/ingestion/pause";

    private PauseIngestionAction() {
        super(NAME, PauseIngestionResponse::new);
    }
}
