/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A single delegated predicate — carries the annotation ID, the accepting backend,
 * and the serialized bytes produced by the accepting backend's
 * {@link DelegatedPredicateSerializer} or anything similar.
 *
 * @opensearch.internal
 */
public class DelegatedExpression implements Writeable {

    private final int annotationId;
    private final String acceptingBackendId;
    private final byte[] expressionBytes;

    public DelegatedExpression(int annotationId, String acceptingBackendId, byte[] expressionBytes) {
        this.annotationId = annotationId;
        this.acceptingBackendId = acceptingBackendId;
        this.expressionBytes = expressionBytes;
    }

    public DelegatedExpression(StreamInput in) throws IOException {
        this.annotationId = in.readInt();
        this.acceptingBackendId = in.readString();
        this.expressionBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(annotationId);
        out.writeString(acceptingBackendId);
        out.writeByteArray(expressionBytes);
    }

    public int getAnnotationId() {
        return annotationId;
    }

    public String getAcceptingBackendId() {
        return acceptingBackendId;
    }

    public byte[] getExpressionBytes() {
        return expressionBytes;
    }
}
