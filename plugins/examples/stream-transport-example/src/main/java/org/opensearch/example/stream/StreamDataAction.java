/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.action.ActionType;

class StreamDataAction extends ActionType<StreamDataResponse> {
    public static final StreamDataAction INSTANCE = new StreamDataAction();
    public static final String NAME = "cluster:admin/stream_data";

    private StreamDataAction() {
        super(NAME, StreamDataResponse::new);
    }
}
