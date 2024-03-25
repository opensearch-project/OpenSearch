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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Script for derived fields
 *
 * @opensearch.internal
 */
public abstract class DerivedFieldScript {

    public static final String[] PARAMETERS = {};

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("derived", Factory.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DynamicMap.class);
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = Map.of("doc", value -> {
        deprecationLogger.deprecate(
            "derived-field-script_doc",
            "Accessing variable [doc] via [params.doc] from within a derived-field-script "
                + "is deprecated in favor of directly accessing [doc]."
        );
        return value;
    }, "_doc", value -> {
        deprecationLogger.deprecate(
            "derived-field-script__doc",
            "Accessing variable [doc] via [params._doc] from within a derived-field-script "
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

    /**
     * The field values emitted from the script.
     */
    private List<Object> emittedValues;

    public DerivedFieldScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        Map<String, Object> parameters = new HashMap<>(params);
        this.leafLookup = lookup.getLeafSearchLookup(leafContext);
        parameters.putAll(leafLookup.asMap());
        this.params = new DynamicMap(parameters, PARAMS_FUNCTIONS);
        this.emittedValues = new ArrayList<>();
    }

    public DerivedFieldScript() {
        this.params = null;
        this.leafLookup = null;
        this.emittedValues = new ArrayList<>();
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
     * Return the emitted values from the script execution.
     */
    public List<Object> getEmittedValues() { return emittedValues; }

    /**
     * Set the current document to run the script on next.
     * Clears the emittedValues as well since they should be scoped per document.
     */
    public void setDocument(int docid) {
        this.emittedValues = new ArrayList<>();
        leafLookup.setDocument(docid);
    }

    public void addEmittedValue(Object o) { emittedValues.add(o); }

    public void execute() {}

    /**
     * A factory to construct {@link DerivedFieldScript} instances.
     *
     * @opensearch.internal
     */
    public interface LeafFactory {
        DerivedFieldScript newInstance(final LeafReaderContext ctx) throws IOException;
    }

    /**
     * A factory to create stateful {@link DerivedFieldScript} factories.
     *
     * @opensearch.internal
     */
    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }
}
