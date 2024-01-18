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
package org.opensearch.index.codec.freshstartree.query;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.CollectorManager;


/** TODO : remove this ? */
public class StarTreeCollectorManager implements CollectorManager<StarTreeCollector, StarTreeCollector> {
    @Override
    public StarTreeCollector newCollector()
        throws IOException {
        return new StarTreeCollector();
    }

    @Override
    public StarTreeCollector reduce(Collection<StarTreeCollector> collectors)
        throws IOException {
        if (collectors == null || collectors.size() == 0) {
            return new StarTreeCollector();
        }
        if (collectors.size() == 1) {
            return collectors.iterator().next();
        }
        return new ReducedStarTreeCollector(collectors);
    }

    private static class ReducedStarTreeCollector extends StarTreeCollector {

        public ReducedStarTreeCollector(final Collection<StarTreeCollector> facetsCollectors) {
            //      final List<MatchingDocs> matchingDocs = this.getMatchingDocs();
            //      facetsCollectors.forEach(
            //          facetsCollector -> matchingDocs.addAll(facetsCollector.getMatchingDocs()));
        }
    }
}
