/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

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
     * @param fieldProducerMap             contains the docValues producer to get docValues associated with each field
     * @throws IOException when we are unable to build star-tree
     */

    void build(Map<String, DocValuesProducer> fieldProducerMap) throws IOException;

    /**
     * Builds the star tree using StarTree values from multiple segments
     *
     * @param starTreeValuesSubs           contains the star tree values from multiple segments
     * @throws IOException when we are unable to build star-tree
     */
    void build(List<StarTreeValues> starTreeValuesSubs) throws IOException;
}
