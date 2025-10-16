/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.fields.number;

import org.apache.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;

public class ByteLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue) {

        NumberFieldMapper.NumberFieldType numberFieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;

        //TODO: check how can we get the skiplist here
//        document.addAll(numberFieldType.numberType().createFields(numberFieldType.name(), parseValue,
//            numberFieldType.isSearchable(), numberFieldType.hasDocValues(), skiplist, numberFieldType.isStored()));

        if (numberFieldType.hasDocValues() == false && (numberFieldType.isStored() || numberFieldType.isSearchable())) {

        }
    }
}
