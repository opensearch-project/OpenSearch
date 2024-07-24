/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;

public abstract class ApproximateConstantScoreWeight extends ConstantScoreWeight {

    protected ApproximateConstantScoreWeight(Query query, float score) {
        super(query, score);
    }

    protected abstract long intersectLeft(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree) throws  IOException;

    protected abstract long intersectRight(PointValues.IntersectVisitor visitor, PointValues.PointTree pointTree) throws IOException;

    protected abstract PointValues.IntersectVisitor getIntersectVisitor(DocIdSetBuilder result) throws IOException;

}
