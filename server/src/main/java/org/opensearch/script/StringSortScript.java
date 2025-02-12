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

package org.opensearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Script for sorting strings
 *
 * @opensearch.internal
 */
public abstract class StringSortScript extends AbstractSortScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("string_sort", Factory.class);

    public StringSortScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
    }

    public abstract String execute();

    /**
     * A factory to construct {@link StringSortScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        StringSortScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link StringSortScript} factories for a specific index.
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
