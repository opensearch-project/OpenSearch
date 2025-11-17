/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.benchmark;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.Closeable;
import java.io.IOException;

public class BenchmarkData implements Closeable {
    private final VectorSchemaRoot root;
    private final ArrowSchema arrowSchema;
    private final ArrowArray arrowArray;

    public BenchmarkData(VectorSchemaRoot root, ArrowSchema arrowSchema, ArrowArray arrowArray) {
        this.root = root;
        this.arrowSchema = arrowSchema;
        this.arrowArray = arrowArray;
    }

    public ArrowSchema getArrowSchema() {
        return arrowSchema;
    }

    public ArrowArray getArrowArray() {
        return arrowArray;
    }

    @Override
    public void close() throws IOException {
        root.close();
        arrowArray.close();
        arrowSchema.close();
    }
}
