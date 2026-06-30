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

package org.opensearch.index.mapper;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.Collections;

public class ICUCollationKeywordFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        ICUCollationKeywordFieldMapper mapper = new ICUCollationKeywordFieldMapper.Builder("field").build(context);
        assertEquals(Collections.singletonList("42"), fetchSourceValue(mapper.fieldType(), 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(mapper.fieldType(), true));

        ICUCollationKeywordFieldMapper ignoreAboveMapper = new ICUCollationKeywordFieldMapper.Builder("field").ignoreAbove(4)
            .build(context);
        assertEquals(Collections.emptyList(), fetchSourceValue(ignoreAboveMapper.fieldType(), "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(ignoreAboveMapper.fieldType(), 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(ignoreAboveMapper.fieldType(), true));

        ICUCollationKeywordFieldMapper nullValueMapper = new ICUCollationKeywordFieldMapper.Builder("field").nullValue("NULL")
            .build(context);
        assertEquals(Collections.singletonList("NULL"), fetchSourceValue(nullValueMapper.fieldType(), null));
    }
}
