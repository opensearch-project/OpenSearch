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

package org.opensearch.templates;

import java.util.Collections;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.script.ScriptService;

/**
 * The OpenSearch templates service to manage individual template services
 *
 * @opensearch.internal
 */
public class TemplatesService {

    private volatile Map<String, TemplateService> templates = Collections.emptyMap();

    public boolean hasTemplate(String templateName) {
        return templates.containsKey(templateName);
    }

    public TemplateService templateService(String templateName) {
        return templates.get(templateName);
    }

    public synchronized TemplateService createTemplateService(
        String templateName,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        NamedXContentRegistry xContentRegistry,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<QueryShardContext> queryShardContextSupplier,
        BooleanSupplier idFieldDataEnabled,
        ScriptService scriptService
    ) {
        final TemplateService templateService = new TemplateService(
            templateName,
            indexSettings,
            indexAnalyzers,
            xContentRegistry,
            similarityService,
            mapperRegistry,
            queryShardContextSupplier,
            idFieldDataEnabled,
            scriptService
        );
        templates = MapBuilder.newMapBuilder(templates).put(templateName, templateService).immutableMap();
        return templateService;
    }
}
