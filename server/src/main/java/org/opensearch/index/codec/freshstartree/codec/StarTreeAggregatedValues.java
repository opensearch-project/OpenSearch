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
package org.opensearch.index.codec.freshstartree.codec;

import java.util.Map;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.index.codec.freshstartree.node.StarTree;

// TODO : this is tightly coupled to star tree

/** Star tree aggregated values holder for reader / query */
public class StarTreeAggregatedValues {
    public StarTree _starTree;
    public Map<String, NumericDocValues> dimensionValues;

    public Map<String, NumericDocValues> metricValues;

    public StarTreeAggregatedValues(StarTree starTree, Map<String, NumericDocValues> dimensionValues,
        Map<String, NumericDocValues> metricValues) {
        this._starTree = starTree;
        this.dimensionValues = dimensionValues;
        this.metricValues = metricValues;
    }
}
