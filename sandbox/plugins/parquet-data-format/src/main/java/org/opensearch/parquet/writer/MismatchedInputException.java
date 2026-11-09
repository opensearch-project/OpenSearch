/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

/**
 * Thrown when a document presents a field whose declared type does not have a
 * corresponding {@link org.opensearch.parquet.fields.ParquetField} mapping in the
 * Arrow field registry — i.e., the input schema is incompatible with the writer's
 * known schema. The failure is per-document and recoverable: callers should surface
 * it as a {@link org.opensearch.index.engine.dataformat.WriteResult.Failure} so the
 * writer can be rolled back and remain usable for subsequent docs.
 */
public class MismatchedInputException extends IllegalArgumentException {

    public MismatchedInputException(String message) {
        super(message);
    }

    public MismatchedInputException(String message, Throwable cause) {
        super(message, cause);
    }
}
