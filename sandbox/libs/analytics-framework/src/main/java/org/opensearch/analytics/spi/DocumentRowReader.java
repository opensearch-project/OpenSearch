/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Backend-specific execution contract for reading document rows. Backends implement this
 * to perform the actual storage read (e.g., DataFusion native parquet scan) from a file set
 * that the Core layer has already resolved from the catalog snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DocumentRowReader {

    /**
     * The storage format this backend reads (e.g. {@code "parquet"}). Lets the Core layer
     * resolve the backend's candidate {@link WriterFileSet} from the catalog snapshot.
     */
    String formatName();

    /**
     * Fetch a single row at the given offset from the pre-resolved file set.
     *
     * @param rowId the row offset to fetch
     * @param fileSet the file set to read from
     * @return the row as a field-name → value map, or null if not found
     */
    Map<String, Object> executeSingleRow(long rowId, WriterFileSet fileSet) throws IOException;

    /**
     * Executes a backend plan constrained to the resolved row. Implementations must not fall
     * back to the unfiltered point lookup when a non-empty plan is supplied.
     *
     * @param rowId the resolved row offset to constrain the plan to
     * @param fileSet the file set containing the resolved row
     * @param tableName the table name referenced by the plan
     * @param plan the serialized backend plan containing the row constraint and access policy
     * @return the row as a field-name → value map, or null when the plan produces no row
     */
    Map<String, Object> executeSingleRow(long rowId, WriterFileSet fileSet, String tableName, byte[] plan) throws IOException;

    /**
     * Fetch all rows with {@code _seq_no > fromSeqNoExclusive} from the Core-resolved file sets
     * (one per segment for this backend's format).
     *
     * @param fileSets the file sets to scan
     * @param fromSeqNoExclusive the exclusive lower bound on {@code _seq_no}
     */
    List<Map<String, Object>> executeRowsAboveSeqNo(List<WriterFileSet> fileSets, long fromSeqNoExclusive) throws IOException;
}
