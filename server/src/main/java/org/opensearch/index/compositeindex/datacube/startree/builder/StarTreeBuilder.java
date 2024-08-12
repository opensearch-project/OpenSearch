/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A star-tree builder that builds a single star-tree.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StarTreeBuilder extends Closeable {
    /**
     * Builds the star tree from the original segment documents
     *
     * @param fieldProducerMap           contains the docValues producer to get docValues associated with each field
     * @param fieldNumberAcrossStarTrees maintains the unique field number across the fields in the star tree
     * @param starTreeDocValuesConsumer  consumer of star-tree doc values
     * @throws IOException when we are unable to build star-tree
     */

    void build(
        Map<String, DocValuesProducer> fieldProducerMap,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException;

    /**
     * Builds the star tree using Tree values from multiple segments
     *
     * @param starTreeValuesSubs           contains the star tree values from multiple segments
     * @param fieldNumberAcrossStarTrees   maintains the unique field number across the fields in the star tree
     * @param starTreeDocValuesConsumer    consumer of star-tree doc values
     * @throws IOException when we are unable to build star-tree
     */
    void build(
        List<StarTreeValues> starTreeValuesSubs,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException;
}
