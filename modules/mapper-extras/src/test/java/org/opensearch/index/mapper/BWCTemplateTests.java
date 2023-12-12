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

package org.opensearch.index.mapper;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;

import static org.opensearch.test.StreamsUtils.copyToBytesFromClasspath;

/**
 * Rudimentary tests that the templates used by Logstash and Beats
 * prior to their 5.x releases work for newly created indices
 */
public class BWCTemplateTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperExtrasModulePlugin.class);
    }

    public void testBeatsTemplatesBWC() throws Exception {
        byte[] metricBeat = copyToBytesFromClasspath("/org/opensearch/index/mapper/metricbeat-6.0.template.json");
        byte[] packetBeat = copyToBytesFromClasspath("/org/opensearch/index/mapper/packetbeat-6.0.template.json");
        byte[] fileBeat = copyToBytesFromClasspath("/org/opensearch/index/mapper/filebeat-6.0.template.json");
        client().admin().indices().preparePutTemplate("metricbeat").setSource(metricBeat, MediaTypeRegistry.JSON).get();
        client().admin().indices().preparePutTemplate("packetbeat").setSource(packetBeat, MediaTypeRegistry.JSON).get();
        client().admin().indices().preparePutTemplate("filebeat").setSource(fileBeat, MediaTypeRegistry.JSON).get();

        client().prepareIndex("metricbeat-foo").setId("1").setSource("message", "foo").get();
        client().prepareIndex("packetbeat-foo").setId("1").setSource("message", "foo").get();
        client().prepareIndex("filebeat-foo").setId("1").setSource("message", "foo").get();
    }
}
