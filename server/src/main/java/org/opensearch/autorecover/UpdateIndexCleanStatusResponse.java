/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autorecover;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamOutput;
import java.io.IOException;

class UpdateIndexCleanStatusResponse extends ActionResponse {

    public static final UpdateIndexCleanStatusResponse INSTANCE = new UpdateIndexCleanStatusResponse();

    UpdateIndexCleanStatusResponse() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}
}
