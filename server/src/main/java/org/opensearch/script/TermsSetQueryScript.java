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
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Script for terms set query
 *
 * @opensearch.internal
 */
public abstract class TermsSetQueryScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("terms_set", Factory.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of("doc", value -> {
        deprecationLogger.deprecate(
            "terms-set-query-script_doc",
            "Accessing variable [doc] via [params.doc] from within an terms-set-query-script "
                + "is deprecated in favor of directly accessing [doc]."
        );
        return value;
    }, "_doc", value -> {
        deprecationLogger.deprecate(
            "terms-set-query-script__doc",
            "Accessing variable [doc] via [params._doc] from within an terms-set-query-script "
                + "is deprecated in favor of directly accessing [doc]."
        );
        return value;
    }, "_source", value -> ((SourceLookup) value).loadSourceIfNeeded());

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    public TermsSetQueryScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
    }

    protected TermsSetQueryScript() {
        params = null;
        leafLookup = null;
    }

    /**
     * Return the parameters for this script.
     */
    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * The doc lookup for the Lucene segment this script was created for.
     */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /**
     * Set the current document to run the script on next.
     */
    public void setDocument(int docid) {
        leafLookup.setDocument(docid);
    }

    /**
     * Return the result as a long. This is used by aggregation scripts over long fields.
     */
    public long runAsLong() {
        return execute().longValue();
    }

    public abstract Number execute();

    /**
     * A factory to construct {@link TermsSetQueryScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        TermsSetQueryScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link TermsSetQueryScript} factories for a specific index.
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
