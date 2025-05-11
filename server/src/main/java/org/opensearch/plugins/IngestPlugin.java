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

package org.opensearch.plugins;

import org.opensearch.ingest.Processor;

import java.util.Collections;
import java.util.Map;

/**
 * An extension point for {@link Plugin} implementations to add custom ingest processors
 *
 * @opensearch.api
 */
public interface IngestPlugin {

    /**
     * Returns additional ingest processor types added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.ingest.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Returns additional system ingest processor types added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the processor, and the value is a {@link org.opensearch.ingest.Processor.Factory}
     * to create the processor systematically.
     *
     */
    default Map<String, Processor.Factory> getSystemIngestProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Define the keys we can use in the config for the system ingest pipeline.
     * Currently we will only systematically generate the ingest pipeline based on the index mapping.
     */
    class SystemIngestPipelineConfigKeys {
        /**
         * Use this key to access the mappings of the index from the config
         * example:
         * {
         *     "_doc":{
         *         "properties":{
         *             "fieldName":{
         *                 "type": "text"
         *             }
         *         }
         *     }
         * }
         */
        public static final String INDEX_MAPPINGS = "index_mappings";

        /**
         *  Use this key to access the mappings of the matched templates of the index from the config. This will be used
         *  for the case when we try to index a doc while the index has not been created. And the index name matches some
         *  templates. In that case we should create the system ingest pipeline based on the matched templates.
         *
         *  If there are multiple matched templates the later one can override the setting of the previous one if merge
         *  rules are allowed. It works like you define the index first and then update it. So it will not be able to
         *  override the field which is not updatable.
         *  example mappings from templates:
         *  [
         *  {
         *      "_doc":{
         *          "properties":{
         *              "fieldName":{
         *                  "type": "text"
         *              }
         *          }
         *      }
         *  },
         *  {...}
         *  ]
         */
        public static final String INDEX_TEMPLATE_MAPPINGS = "index_template_mappings";
    }
}
