/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.queries.intervals.IntervalIterator;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptFactory;

/**
 * Base class for scripts used as interval filters, see {@link IntervalsSourceProvider.IntervalFilter}
 *
 * @opensearch.internal
 */
public abstract class IntervalFilterScript {

    /**
     * Internal interval
     *
     * @opensearch.internal
     */
    public static class Interval {

        private IntervalIterator iterator;

        void setIterator(IntervalIterator iterator) {
            this.iterator = iterator;
        }

        public int getStart() {
            return iterator.start();
        }

        public int getEnd() {
            return iterator.end();
        }

        public int getGaps() {
            return iterator.gaps();
        }
    }

    public abstract boolean execute(Interval interval);

    /**
     * Factory to create a script
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        IntervalFilterScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] { "interval" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("interval", Factory.class);

}
