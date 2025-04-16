/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.resume;

import org.opensearch.action.ActionType;

/**
 * Transport action for resuming ingestion.
 *
 * @opensearch.experimental
 */
public class ResumeIngestionAction extends ActionType<ResumeIngestionResponse> {

    public static final ResumeIngestionAction INSTANCE = new ResumeIngestionAction();
    public static final String NAME = "indices:admin/ingestion/resume";

    private ResumeIngestionAction() {
        super(NAME, ResumeIngestionResponse::new);
    }
}
