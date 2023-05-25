/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.ProtobufOpenSearchException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * This exception can be used to wrap a given, not serializable exception
* to serialize via {@link ProtobufStreamOutput#writeException(Throwable)}.
* This class will preserve the stacktrace as well as the suppressed exceptions of
* the throwable it was created with instead of it's own. The stacktrace has no indication
* of where this exception was created.
*
* @opensearch.internal
*/
public final class ProtobufNotSerializableExceptionWrapper extends ProtobufOpenSearchException {

    private final String name;
    private final RestStatus status;

    public ProtobufNotSerializableExceptionWrapper(Throwable other) {
        super(ProtobufOpenSearchException.getExceptionName(other) + ": " + other.getMessage(), other.getCause());
        this.name = ProtobufOpenSearchException.getExceptionName(other);
        this.status = ExceptionsHelper.status(other);
        setStackTrace(other.getStackTrace());
        for (Throwable otherSuppressed : other.getSuppressed()) {
            addSuppressed(otherSuppressed);
        }
        if (other instanceof ProtobufOpenSearchException) {
            ProtobufOpenSearchException ex = (ProtobufOpenSearchException) other;
            for (String key : ex.getHeaderKeys()) {
                this.addHeader(key, ex.getHeader(key));
            }
            for (String key : ex.getMetadataKeys()) {
                this.addMetadata(key, ex.getMetadata(key));
            }
        }
    }

    public ProtobufNotSerializableExceptionWrapper(CodedInputStream in) throws IOException {
        super(in);
        name = in.readString();
        status = RestStatus.readFromProtobuf(in);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        out.writeStringNoTag(name);
        RestStatus.writeToProtobuf(out, status);
    }

    @Override
    protected String getExceptionName() {
        return name;
    }

    @Override
    public RestStatus status() {
        return status;
    }
}
