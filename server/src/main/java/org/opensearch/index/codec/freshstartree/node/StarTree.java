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
import java.util.List;
import java.util.Map;


/** Interface for star tree */
public interface StarTree {

    /** Get the root node of the star tree. */
    StarTreeNode getRoot();

    /**
     * Get a list of all dimension names. The node dimension id is the index of the dimension name in
     * this list.
     */
    List<String> getDimensionNames();

    void printTree(Map<String, Map<String, String>> dictionaryMap)
        throws IOException;
}
