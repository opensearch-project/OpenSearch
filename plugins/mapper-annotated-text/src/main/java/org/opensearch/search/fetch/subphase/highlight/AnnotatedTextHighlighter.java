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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.opensearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AnnotatedTextHighlighter extends UnifiedHighlighter {

    public static final String NAME = "annotated";

    // Convert the marked-up values held on-disk to plain-text versions for highlighting
    @Override
    protected List<Object> loadFieldValues(
        CustomUnifiedHighlighter highlighter,
        QueryShardContext context,
        MappedFieldType fieldType,
        HitContext hitContext,
        boolean forceSource
    ) throws IOException {
        List<Object> fieldValues = super.loadFieldValues(highlighter, context, fieldType, hitContext, forceSource);

        List<Object> strings = new ArrayList<>(fieldValues.size());
        AnnotatedText[] annotations = new AnnotatedText[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            annotations[i] = AnnotatedText.parse(fieldValues.get(i).toString());
            strings.add(annotations[i].textMinusMarkup);
        }
        // Store the annotations in the formatter and analyzer
        ((AnnotatedPassageFormatter) highlighter.getFormatter()).setAnnotations(annotations);
        ((AnnotatedHighlighterAnalyzer) highlighter.getIndexAnalyzer()).setAnnotations(annotations);
        return strings;
    }

    @Override
    protected Analyzer getAnalyzer(DocumentMapper docMapper) {
        return new AnnotatedHighlighterAnalyzer(super.getAnalyzer(docMapper));
    }

    @Override
    protected PassageFormatter getPassageFormatter(HitContext hitContext, SearchHighlightContext.Field field, Encoder encoder) {
        return new AnnotatedPassageFormatter(encoder);
    }

}
