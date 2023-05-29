/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.common.io.stream.BytesStream;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/**
 * Server specific utilities for BytesReference objects
 *
 * @opensearch.internal
 */
public class BytesReferenceUtil {
    /**
     * Convert an {@link XContentBuilder} into a BytesReference. This method closes the builder,
     * so no further fields may be added.
     */
    public static BytesReference bytes(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream) {
            return new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            return ((BytesStream) stream).bytes();
        }
    }
}
