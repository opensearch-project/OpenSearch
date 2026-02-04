/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.core.data.date;

import org.apache.lucene.document.LongPoint;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public class DateLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {
        final DateFieldMapper.DateFieldType fieldType = (DateFieldMapper.DateFieldType) mappedFieldType;
        final Long timestamp = (Long) parseValue;
        if (fieldType.isSearchable()) {
            document.add(new LongPoint(fieldType.name(), timestamp));
        }
    }
}
