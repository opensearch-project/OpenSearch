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
 * ValueFetcher used by Derived Fields.
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
        // TODO: remove List.of() when derivedFieldScript.execute() returns list of objects.
        return List.of(derivedFieldScript.execute());
    }

    public void setNextReader(LeafReaderContext context) {
        try {
            derivedFieldScript = derivedFieldScriptFactory.newInstance(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
