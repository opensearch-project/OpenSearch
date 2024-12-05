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

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.script.FieldScript;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Gets the scripted fields during search phase
 *
 * @opensearch.internal
 */
public final class ScriptFieldsPhase implements FetchSubPhase {

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.scriptFields() == null) {
            return null;
        }
        List<ScriptFieldsContext.ScriptField> scriptFields = context.scriptFields().fields();
        return new FetchSubPhaseProcessor() {

            FieldScript[] leafScripts = null;

            @Override
            public void setNextReader(LeafReaderContext readerContext) {
                leafScripts = createLeafScripts(readerContext, scriptFields);
            }

            @Override
            public void process(HitContext hitContext) {
                int docId = hitContext.docId();
                for (int i = 0; i < leafScripts.length; i++) {
                    leafScripts[i].setDocument(docId);
                    final Object value;
                    try {
                        value = leafScripts[i].execute();
                        CollectionUtils.ensureNoSelfReferences(value, "ScriptFieldsPhase leaf script " + i);
                    } catch (RuntimeException e) {
                        if (scriptFields.get(i).ignoreException()) {
                            continue;
                        }
                        throw e;
                    }
                    String scriptFieldName = scriptFields.get(i).name();
                    DocumentField hitField = hitContext.hit().field(scriptFieldName);
                    if (hitField == null) {
                        final List<Object> values;
                        if (value instanceof Collection) {
                            values = new ArrayList<>((Collection<?>) value);
                        } else {
                            values = Collections.singletonList(value);
                        }
                        hitField = new DocumentField(scriptFieldName, values);
                        // script fields are never meta-fields
                        hitContext.hit().setDocumentField(scriptFieldName, hitField);
                    }
                }
            }
        };
    }

    private FieldScript[] createLeafScripts(LeafReaderContext context, List<ScriptFieldsContext.ScriptField> scriptFields) {
        FieldScript[] scripts = new FieldScript[scriptFields.size()];
        for (int i = 0; i < scripts.length; i++) {
            try {
                scripts[i] = scriptFields.get(i).script().newInstance(context);
            } catch (IOException e1) {
                throw new IllegalStateException("Failed to load script " + scriptFields.get(i).name(), e1);
            }
        }
        return scripts;
    }
}
