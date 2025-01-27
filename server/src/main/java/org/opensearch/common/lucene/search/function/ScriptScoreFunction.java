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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorable;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.script.ExplainableScoreScript;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Objects;

/**
 * Script score function for search.
 *
 * @opensearch.internal
 */
public class ScriptScoreFunction extends ScoreFunction {

    static final class CannedScorer extends Scorable {
        protected float score;

        @Override
        public float score() {
            return score;
        }
    }

    private final Script sScript;

    private final ScoreScript.LeafFactory script;

    private final int shardId;
    private final String indexName;
    private final Version indexVersion;
    private final String functionName;

    public ScriptScoreFunction(
        Script sScript,
        ScoreScript.LeafFactory script,
        String indexName,
        int shardId,
        Version indexVersion,
        @Nullable String functionName
    ) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.script = script;
        this.indexName = indexName;
        this.shardId = shardId;
        this.indexVersion = indexVersion;
        this.functionName = functionName;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final ScoreScript leafScript = script.newInstance(ctx);
        final CannedScorer scorer = new CannedScorer();
        leafScript.setScorer(scorer);
        leafScript._setIndexName(indexName);
        leafScript._setShard(shardId);
        leafScript._setIndexVersion(indexVersion);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                leafScript.setDocument(docId);
                scorer.score = subQueryScore;
                double result = leafScript.execute(null);
                if (result < 0f) {
                    throw new IllegalArgumentException("script score function must not produce negative scores, but got: [" + result + "]");
                }
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation exp;
                if (leafScript instanceof ExplainableScoreScript) {
                    leafScript.setDocument(docId);
                    scorer.score = subQueryScore.getValue().floatValue();
                    exp = ((ExplainableScoreScript) leafScript).explain(subQueryScore, functionName);
                } else {
                    double score = score(docId, subQueryScore.getValue().floatValue());
                    // info about params already included in sScript
                    String explanation = "script score function"
                        + Functions.nameOrEmptyFunc(functionName)
                        + ", computed with script:\""
                        + sScript
                        + "\"";
                    Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
                    return Explanation.match((float) score, explanation, scoreExp);
                }
                return exp;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return script.needs_score();
    }

    @Override
    public String toString() {
        return "script" + sScript.toString();
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        ScriptScoreFunction scriptScoreFunction = (ScriptScoreFunction) other;
        return Objects.equals(this.sScript, scriptScoreFunction.sScript);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sScript);
    }
}
