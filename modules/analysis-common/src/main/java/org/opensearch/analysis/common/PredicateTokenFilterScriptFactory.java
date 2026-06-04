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

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenFilterFactory;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;

import java.io.IOException;

/**
 * A factory for creating FilteringTokenFilters that determine whether or not to
 * accept their underlying token by consulting a script
 */
public class PredicateTokenFilterScriptFactory extends AbstractTokenFilterFactory {

    private final AnalysisPredicateScript.Factory factory;

    public PredicateTokenFilterScriptFactory(IndexSettings indexSettings, String name, Settings settings, ScriptService scriptService) {
        super(indexSettings, name, settings);
        Settings scriptSettings = settings.getAsSettings("script");
        Script script = Script.parse(scriptSettings);
        if (script.getType() != ScriptType.INLINE) {
            throw new IllegalArgumentException("Cannot use stored scripts in tokenfilter [" + name + "]");
        }
        this.factory = scriptService.compile(script, AnalysisPredicateScript.CONTEXT);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new ScriptFilteringTokenFilter(tokenStream, factory.newInstance());
    }

    private static class ScriptFilteringTokenFilter extends FilteringTokenFilter {

        final AnalysisPredicateScript script;
        final AnalysisPredicateScript.Token token;

        ScriptFilteringTokenFilter(TokenStream in, AnalysisPredicateScript script) {
            super(in);
            this.script = script;
            this.token = new AnalysisPredicateScript.Token(this);
        }

        @Override
        protected boolean accept() throws IOException {
            token.updatePosition();
            return script.execute(token);
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            this.token.reset();
        }
    }
}
