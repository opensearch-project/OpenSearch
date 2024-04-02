/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

/**
 * The value fetcher contains logic to execute script and fetch the value in form of list of object.
 * It expects DerivedFieldScript.LeafFactory as an input and sets the contract with consumer to call
 * {@link #setNextReader(LeafReaderContext)} whenever a segment is switched.
 */
public final class DerivedFieldValueFetcher implements ValueFetcher {
    private DerivedFieldScript derivedFieldScript;
    private final DerivedFieldScript.LeafFactory derivedFieldScriptFactory;

    public DerivedFieldValueFetcher(DerivedFieldScript.LeafFactory derivedFieldScriptFactory) {
        this.derivedFieldScriptFactory = derivedFieldScriptFactory;
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) {
        derivedFieldScript.setDocument(lookup.docId());
        derivedFieldScript.execute();
        return derivedFieldScript.getEmittedValues();
    }

    public void setNextReader(LeafReaderContext context) {
        try {
            derivedFieldScript = derivedFieldScriptFactory.newInstance(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
