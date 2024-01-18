/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.node;

import java.io.IOException;
import java.util.Iterator;


/** Class representing each node in star tree */
public interface StarTreeNode {
    long ALL = -1l;

    /** Get the index of the dimension. */
    int getDimensionId()
        throws IOException;

    /** Get the value (dictionary id) of the dimension. */
    long getDimensionValue()
        throws IOException;

    /** Get the child dimension id. */
    int getChildDimensionId()
        throws IOException;

    /** Get the index of the start document. */
    int getStartDocId()
        throws IOException;

    /** Get the index of the end document (exclusive). */
    int getEndDocId()
        throws IOException;

    /** Get the index of the aggregated document. */
    int getAggregatedDocId()
        throws IOException;

    /** Get the number of children nodes. */
    int getNumChildren()
        throws IOException;

    /** Return true if the node is a leaf node, false otherwise. */
    boolean isLeaf();

    /**
     * Get the child node corresponding to the given dimension value (dictionary id), or null if such
     * child does not exist.
     */
    StarTreeNode getChildForDimensionValue(long dimensionValue)
        throws IOException;

    /** Get the iterator over all children nodes. */
    Iterator<? extends StarTreeNode> getChildrenIterator()
        throws IOException;
}
