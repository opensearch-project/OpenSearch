/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Per-request registry of the dedicated cursors one request opened through its
 * {@link ParquetDocValuesLeafReader}.
 *
 * <p>The doc-values accessor API ({@code getSortedNumericDocValues(field)}) carries no request
 * identity, and under concurrent segment search the slice threads serving one request do not map
 * back to it, so the request-scoped wrapper is the only place that knows which cursors to close when
 * the request ends. The segment-lifetime producer is shared across requests and is not closed here.
 */
final class CursorRegistry implements Closeable {

    private final List<ParquetColumnReader> cursors = Collections.synchronizedList(new ArrayList<>());

    /**
     * Records a cursor this request opened; it is closed by {@link #close()} at request end. Every
     * accessor call on this request's leaf readers completes before the searcher closes, so no cursor
     * is registered after {@link #close()}.
     */
    void register(ParquetColumnReader cursor) {
        cursors.add(cursor);
    }

    @Override
    public void close() throws IOException {
        synchronized (cursors) {
            for (ParquetColumnReader cursor : cursors) {
                // Idempotent through NativeHandle; close never throws checked exceptions.
                cursor.close();
            }
            cursors.clear();
        }
    }

    /** Cursors opened on this request (tests). */
    List<ParquetColumnReader> opened() {
        synchronized (cursors) {
            return List.copyOf(cursors);
        }
    }
}
