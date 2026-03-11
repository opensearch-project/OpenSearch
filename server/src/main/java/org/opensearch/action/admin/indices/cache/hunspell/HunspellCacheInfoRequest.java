/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.cache.hunspell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for retrieving Hunspell cache information.
 *
 * @opensearch.internal
 */
public class HunspellCacheInfoRequest extends ActionRequest {

    public HunspellCacheInfoRequest() {}

    public HunspellCacheInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
