/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

class XContentHttpChunk implements HttpChunk {
    private final BytesReference content;

    public XContentHttpChunk(final XContentBuilder builder) {
        if (builder == XContentBuilder.NO_CONTENT) {
            content = BytesArray.EMPTY;
        } else {
            content = BytesReference.bytes(builder);
        }
    }

    @Override
    public boolean isLast() {
        return content == BytesArray.EMPTY;
    }

    @Override
    public BytesReference content() {
        return content;
    }

    @Override
    public void release() {}
}
