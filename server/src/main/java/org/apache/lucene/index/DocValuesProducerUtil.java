/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.codecs.DocValuesProducer;

import java.util.Collections;
import java.util.Set;

/**
 * Utility class for DocValuesProducers
 * @opensearch.internal
 */
public class DocValuesProducerUtil {
    /**
     * Returns the segment doc values producers for the given doc values producer.
     * If the given doc values producer is not a segment doc values producer, an empty set is returned.
     * @param docValuesProducer the doc values producer
     * @return the segment doc values producers
     */
    public static Set<DocValuesProducer> getSegmentDocValuesProducers(DocValuesProducer docValuesProducer) {
        if (docValuesProducer instanceof SegmentDocValuesProducer) {
            return (((SegmentDocValuesProducer) docValuesProducer).dvProducers);
        }
        return Collections.emptySet();
    }
}
