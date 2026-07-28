/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;

/**
 * Read-only Lucene {@link DocValuesFormat} that serves doc values from Parquet for fields the
 * composite engine has marked as Parquet-resident.
 *
 * <p>Registered via Lucene SPI ({@code META-INF/services/org.apache.lucene.codecs.DocValuesFormat})
 * under the name {@value #FORMAT_NAME}. Lucene's {@code PerFieldDocValuesFormat} routes a field
 * to this format when the segment's {@code FieldInfo} carries
 * {@code PerFieldDocValuesFormat.format = "ParquetDocValues"} (stamped by the composite codec).
 *
 * <h2>Configuration</h2>
 * Lucene SPI requires a public no-arg constructor; that instance is used only for name-based
 * SPI discovery. The functional instance is created with a {@link MapperService} (via
 * {@link #ParquetDocValuesFormat(MapperService)}) by the composite codec, which is needed by
 * {@link ParquetDocValuesProducer} to resolve OpenSearch mapping types for DV-type validation.
 * The Parquet file path is resolved per-segment by {@link ParquetSegmentLayout}.
 */
public final class ParquetDocValuesFormat extends DocValuesFormat {

    /** SPI name of this format. */
    public static final String FORMAT_NAME = "ParquetDocValues";

    private final MapperService mapperService;

    /** Required by Lucene SPI. The resulting instance is used only for name lookup. */
    public ParquetDocValuesFormat() {
        this(null);
    }

    /** Functional constructor used by the composite codec. */
    public ParquetDocValuesFormat(MapperService mapperService) {
        super(FORMAT_NAME);
        this.mapperService = mapperService;
    }

    /** Read-only: the composite indexing engine is the sole writer of Parquet files. */
    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) {
        throw new UnsupportedOperationException("ParquetDocValuesFormat is read-only; writes go through the composite indexing engine.");
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ParquetDocValuesProducer(state, mapperService);
    }
}
