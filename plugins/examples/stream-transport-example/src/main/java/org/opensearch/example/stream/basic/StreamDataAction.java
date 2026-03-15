/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.basic;

import org.opensearch.action.ActionType;

/**
 * Example class
 */
/** Example */
public class StreamDataAction extends ActionType<StreamDataResponse> {
    /** Method */
    public static final StreamDataAction INSTANCE = new StreamDataAction();
    /** Method */
    public static final String NAME = "cluster:admin/stream_data";

    private StreamDataAction() {
        super(NAME, StreamDataResponse::new);
    }
}
