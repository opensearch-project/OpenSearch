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

package org.opensearch.search.aggregations.support.values;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.ScorerAware;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortingBinaryDocValues;
import org.opensearch.script.AggregationScript;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;

/**
 * {@link SortedBinaryDocValues} implementation that reads values from a script.
 *
 * @opensearch.internal
 */
public class ScriptBytesValues extends SortingBinaryDocValues implements ScorerAware {

    private final AggregationScript script;

    public ScriptBytesValues(AggregationScript script) {
        super();
        this.script = script;
    }

    private void set(int i, Object o) {
        if (o == null) {
            values[i].clear();
        } else {
            CollectionUtils.ensureNoSelfReferences(o, "ScriptBytesValues value");
            if (o instanceof BytesRef) {
                values[i].copyBytes((BytesRef) o);
            } else {
                values[i].copyChars(o.toString());
            }
        }
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
        script.setDocument(doc);
        final Object value = script.execute();
        if (value == null) {
            return false;
        } else if (value.getClass().isArray()) {
            count = Array.getLength(value);
            if (count == 0) {
                return false;
            }
            grow();
            for (int i = 0; i < count; ++i) {
                set(i, Array.get(value, i));
            }
        } else if (value instanceof Collection) {
            final Collection<?> coll = (Collection<?>) value;
            count = coll.size();
            if (count == 0) {
                return false;
            }
            grow();
            int i = 0;
            for (Object v : coll) {
                set(i++, v);
            }
        } else {
            count = 1;
            set(0, value);
        }
        sort();
        return true;
    }

    @Override
    public void setScorer(Scorable scorer) {
        script.setScorer(scorer);
    }
}
