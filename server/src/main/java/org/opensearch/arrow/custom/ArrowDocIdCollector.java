/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.custom;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.arrow.spi.StreamProducer;

import java.io.IOException;

/**
 * Collects docIDs from a search and writes them to a VectorSchemaRoot.
 */
public class ArrowDocIdCollector extends FilterCollector {
    private final VectorSchemaRoot root;
    private final StreamProducer.FlushSignal flushSignal;
    private final int batchSize;
    private final IntVector docIDVector;
    private int currentRow;

    public ArrowDocIdCollector(Collector in, VectorSchemaRoot root, StreamProducer.FlushSignal flushSignal, int batchSize) {
        super(in);
        this.root = root;
        this.docIDVector = (IntVector) root.getVector("docID");
        this.flushSignal = flushSignal;
        this.batchSize = batchSize;
        this.currentRow = 0;
    }

    @Override
    public void setWeight(Weight weight) {
        if (this.in != null) {
            this.in.setWeight(weight);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.TOP_DOCS;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        LeafCollector inner = (this.in == null ? null : super.getLeafCollector(context));
        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (inner != null) {
                    inner.setScorer(scorer);
                }
            }

            @Override
            public void collect(int doc) throws IOException {
                if (inner != null) {
                    inner.collect(doc);
                }
                docIDVector.setSafe(currentRow, doc);
                currentRow++;
                if (currentRow >= batchSize) {
                    root.setRowCount(batchSize);
                    flushSignal.awaitConsumption(1000);
                    currentRow = 0;
                }
            }

            @Override
            public void finish() throws IOException {
                if (currentRow > 0) {
                    root.setRowCount(currentRow);
                    flushSignal.awaitConsumption(1000);
                    currentRow = 0;
                }
            }
        };
    }
}
