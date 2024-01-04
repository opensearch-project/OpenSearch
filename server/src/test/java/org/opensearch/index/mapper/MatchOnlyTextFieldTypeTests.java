/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.lucene.Lucene;

public class MatchOnlyTextFieldTypeTests extends TextFieldTypeTests {

    @Override
    TextFieldMapper.TextFieldType createFieldType(boolean searchable) {
        TextSearchInfo tsi = new TextSearchInfo(
            TextFieldMapper.Defaults.FIELD_TYPE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER
        );
        return new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            searchable,
            false,
            tsi,
            ParametrizedFieldMapper.Parameter.metaParam().get()
        );
    }
}
