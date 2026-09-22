/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.action.ActionType;

class NativeArrowStreamDataAction extends ActionType<NativeArrowStreamDataResponse> {
    public static final NativeArrowStreamDataAction INSTANCE = new NativeArrowStreamDataAction();
    public static final String NAME = "cluster:admin/native_arrow_stream_data";

    private NativeArrowStreamDataAction() {
        super(NAME, NativeArrowStreamDataResponse::new);
    }
}
