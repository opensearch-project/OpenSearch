/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

/**
 * Signals that an existing Arrow vector changed type and the current Parquet file must be
 * finalized before indexing can continue with the new schema.
 */
public class SchemaChangeRequiresWriterRotationException extends RuntimeException {

    public SchemaChangeRequiresWriterRotationException(String fieldName, Object previousType, Object nextType) {
        super("Field [" + fieldName + "] changed Parquet type from [" + previousType + "] to [" + nextType + "]");
    }
}
