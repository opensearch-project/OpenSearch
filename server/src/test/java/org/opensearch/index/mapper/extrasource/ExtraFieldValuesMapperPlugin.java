/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper.extrasource;

import org.opensearch.index.mapper.Mapper;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExtraFieldValuesMapperPlugin extends Plugin implements MapperPlugin {
    public static final String EXTRA_FIELDS_TEST = "extra_fields_test";

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>();
        mappers.put(EXTRA_FIELDS_TEST, ExtraTestFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }
}
