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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.opensearch.index.fieldvisitor.CustomFieldsVisitor;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;

public final class HighlightUtils {

    // U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (unified highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;
    public static final char NULL_SEPARATOR = '\u0000';

    private HighlightUtils() {

    }

    /**
     * Load field values for highlighting.
     */
    public static List<Object> loadFieldValues(MappedFieldType fieldType, FetchSubPhase.HitContext hitContext, boolean forceSource)
        throws IOException {
        // percolator needs to always load from source, thus it sets the global force source to true
        List<Object> textsToHighlight;
        if (forceSource == false && fieldType.isStored()) {
            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(singleton(fieldType.name()), false);
            hitContext.reader().document(hitContext.docId(), fieldVisitor);
            textsToHighlight = fieldVisitor.fields().get(fieldType.name());
            if (textsToHighlight == null) {
                // Can happen if the document doesn't have the field to highlight
                textsToHighlight = Collections.emptyList();
            }
        } else {
            SourceLookup sourceLookup = hitContext.sourceLookup();
            textsToHighlight = sourceLookup.extractRawValues(fieldType.name());
        }
        assert textsToHighlight != null;
        return textsToHighlight;
    }

    public static class Encoders {
        public static final Encoder DEFAULT = new DefaultEncoder();
        public static final Encoder HTML = new SimpleHTMLEncoder();
    }

}
