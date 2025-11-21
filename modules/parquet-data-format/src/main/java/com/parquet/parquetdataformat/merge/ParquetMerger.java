/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.RowIdMapping;

import java.util.Collection;

public abstract class ParquetMerger implements Merger {
    @Override
    public MergeResult merge(Collection<FileMetadata> fileMetadataList, RowIdMapping rowIdMapping, long writerGeneration) {
        throw new UnsupportedOperationException("Not supported parquet as secondary data format yet.");
    }
}
