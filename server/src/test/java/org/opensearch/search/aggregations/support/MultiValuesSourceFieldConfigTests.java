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

package org.opensearch.search.aggregations.support;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.search.SearchModule;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MultiValuesSourceFieldConfigTests extends AbstractSerializingTestCase<MultiValuesSourceFieldConfig> {

    @Override
    protected MultiValuesSourceFieldConfig doParseInstance(XContentParser parser) throws IOException {
        return MultiValuesSourceFieldConfig.PARSER.apply(true, true, true).apply(parser, null).build();
    }

    @Override
    protected MultiValuesSourceFieldConfig createTestInstance() {
        String field = randomAlphaOfLength(10);
        Object missing = randomBoolean() ? randomAlphaOfLength(10) : null;
        ZoneId timeZone = randomBoolean() ? randomZone() : null;
        QueryBuilder filter = randomBoolean() ? QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)) : null;
        return new MultiValuesSourceFieldConfig.Builder().setFieldName(field)
            .setMissing(missing)
            .setScript(null)
            .setTimeZone(timeZone)
            .setFilter(filter)
            .build();
    }

    @Override
    protected Writeable.Reader<MultiValuesSourceFieldConfig> instanceReader() {
        return MultiValuesSourceFieldConfig::new;
    }

    public void testMissingFieldScript() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MultiValuesSourceFieldConfig.Builder().build());
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be null.  Please specify one or the other."));
    }

    public void testBothFieldScript() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MultiValuesSourceFieldConfig.Builder().setFieldName("foo").setScript(new Script("foo")).build()
        );
        assertThat(e.getMessage(), equalTo("[field] and [script] cannot both be configured.  Please specify one or the other."));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
    }
}
