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

package org.opensearch.search.rescore;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RescoreProcessor of a search request, used to run potentially expensive scoring models against the top matching documents.
 *
 * @opensearch.internal
 */
public class RescoreProcessor {

    /** Collapse information needed to rebuild a {@link CollapseTopFieldDocs} after rescoring. */
    private record CollapseSnapShot(String field, SortField[] sortFields, Map<Integer, Object> docIdToCollapseValue) {
        static CollapseSnapShot capture(CollapseTopFieldDocs docs) {
            Map<Integer, Object> map = new HashMap<>();
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                map.put(
                    docs.scoreDocs[i].doc,
                    docs.collapseValues[i]
                );
            }
            return new CollapseSnapShot(docs.field, docs.fields, map);
        }
    }

    public void process(SearchContext context) {
        TopDocs topDocs = context.queryResult().topDocs().topDocs;
        if (topDocs.scoreDocs.length == 0) {
            return;
        }
        // Capture the collapse information before rescoring reorders the score docs in place.
        CollapseSnapShot collapseSnapShot = null;
        if (topDocs instanceof CollapseTopFieldDocs docs) {
            collapseSnapShot = CollapseSnapShot.capture(docs);
        }
        try {
            for (RescoreContext ctx : context.rescore()) {
                topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                // It is the responsibility of the rescorer to sort the resulted top docs,
                // here we only assert that this condition is met.
                assert context.sort() == null && topDocsSortedByScore(topDocs) : "topdocs should be sorted after rescore";
            }
            // Rescoring drops the collapse information, so reconstruct the CollapseTopFieldDocs from the snapshot.
            if (collapseSnapShot != null) {
                topDocs = reconstructCollapseTopFieldDocs(
                    collapseSnapShot,
                    topDocs
                );
            }
            context.queryResult()
                .topDocs(new TopDocsAndMaxScore(topDocs, topDocs.scoreDocs[0].score), context.queryResult().sortValueFormats());
        } catch (IOException e) {
            throw new OpenSearchException("Rescore Phase Failed", e);
        }
    }

    /**
     * Returns true if the provided docs are sorted by score.
     */
    private boolean topDocsSortedByScore(TopDocs topDocs) {
        if (topDocs == null || topDocs.scoreDocs == null || topDocs.scoreDocs.length < 2) {
            return true;
        }
        float lastScore = topDocs.scoreDocs[0].score;
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
            ScoreDoc doc = topDocs.scoreDocs[i];
            if (Float.compare(doc.score, lastScore) > 0) {
                return false;
            }
            lastScore = doc.score;
        }
        return true;
    }

    private static CollapseTopFieldDocs reconstructCollapseTopFieldDocs(
        CollapseSnapShot snapShot,
        TopDocs rescoredTopDocs
    ) {
        // collapse + rescore is always score-sorted (rescore cannot be combined with sort), so the sort value
        // built below is a single score. This assertion guards that precondition.
        assert snapShot.sortFields().length == 1 && SortField.FIELD_SCORE.equals(snapShot.sortFields()[0])
            : "rescore must always sort by score descending";
        Object[] newCollapseValues = new Object[rescoredTopDocs.scoreDocs.length];
        FieldDoc[] newFieldDocs = new FieldDoc[rescoredTopDocs.scoreDocs.length];
        for (int i = 0; i < rescoredTopDocs.scoreDocs.length; i++) {
            ScoreDoc scoreDoc = rescoredTopDocs.scoreDocs[i];
            if (!snapShot.docIdToCollapseValue().containsKey(scoreDoc.doc)) {
                throw new IllegalStateException("rescore must not introduce new docs, but doc " + scoreDoc.doc + " was not in original results");
            }
            newCollapseValues[i] = snapShot.docIdToCollapseValue().get(scoreDoc.doc);
            // Carry the rescored score in the sort value: CollapseTopFieldDocs.merge orders by FieldDoc.fields.
            newFieldDocs[i] = new FieldDoc(scoreDoc.doc, scoreDoc.score, new Object[] { scoreDoc.score });
        }
        return new CollapseTopFieldDocs(
            snapShot.field(),
            rescoredTopDocs.totalHits,
            newFieldDocs,
            snapShot.sortFields(),
            newCollapseValues
        );
    }
}
