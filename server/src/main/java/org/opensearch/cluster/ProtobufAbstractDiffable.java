/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.cluster;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.Nullable;

import java.io.IOException;

/**
 * Abstract diffable object with simple diffs implementation that sends the entire object if object has changed or
* nothing if object remained the same.
*
* @opensearch.internal
*/
public abstract class ProtobufAbstractDiffable<T extends ProtobufDiffable<T>> implements ProtobufDiffable<T> {

    private static final ProtobufDiff<?> EMPTY = new CompleteDiff<>();

    @SuppressWarnings("unchecked")
    @Override
    public ProtobufDiff<T> diff(T previousState) {
        if (this.equals(previousState)) {
            return (ProtobufDiff<T>) EMPTY;
        } else {
            return new CompleteDiff<>((T) this);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends ProtobufDiffable<T>> ProtobufDiff<T> readDiffFrom(Reader<T> reader, CodedInputStream in) throws IOException {
        if (in.readBool()) {
            return new CompleteDiff<>(reader.read(in));
        }
        return (ProtobufDiff<T>) EMPTY;
    }

    /**
     * A complete diff.
    *
    * @opensearch.internal
    */
    private static class CompleteDiff<T extends ProtobufDiffable<T>> implements ProtobufDiff<T> {

        @Nullable
        private final T part;

        /**
         * Creates simple diff with changes
        */
        CompleteDiff(T part) {
            this.part = part;
        }

        /**
         * Creates simple diff without changes
        */
        CompleteDiff() {
            this.part = null;
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            if (part != null) {
                out.writeBoolNoTag(true);
                part.writeTo(out);
            } else {
                out.writeBoolNoTag(false);
            }
        }

        @Override
        public T apply(T part) {
            if (this.part != null) {
                return this.part;
            } else {
                return part;
            }
        }
    }
}
