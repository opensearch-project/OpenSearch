/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
 * Definition of Script for DerivedField.
 * It will be used to execute scripts defined against derived fields of any type
 *
 * @opensearch.internal
 */
public abstract class DerivedFieldScript {

    public static final String[] PARAMETERS = {};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("derived_field", Factory.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);

    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of(
        "doc",
        value -> value,
        "_source",
        value -> ((SourceLookup) value).loadSourceIfNeeded()
    );

    /**
     * The generic runtime parameters for the script.
     */
    private final Map<String, Object> params;

    /**
     * A leaf lookup for the bound segment this script will operate on.
     */
    private final LeafSearchLookup leafLookup;

    public DerivedFieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
    }

    protected DerivedFieldScript() {
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

    public abstract Object execute();

    /**
     * A factory to construct {@link DerivedFieldScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        DerivedFieldScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to construct stateful {@link DerivedFieldScript} factories for a specific index.
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
