/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.fields.core.data.text;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.fields.LuceneField;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;

import java.net.InetAddress;

/**
 * Lucene field for IP address values.
 * Conditionally adds an {@link IpFieldMapper.InetAddressField} (combined point + doc values),
 * or individual {@link InetAddressPoint} / {@link SortedSetDocValuesField} components,
 * and a {@link StoredField} for retrieval, matching the pattern in
 * {@link IpFieldMapper#parseCreateField}.
 */
public class IpLuceneField extends LuceneField {

    /** Creates a new IpLuceneField. */
    public IpLuceneField() {}

    @Override
    protected void addToDocument(MappedFieldType fieldType, Document document, Object parseValue) {
        InetAddress address = (InetAddress) parseValue;
        if (fieldType.isSearchable() && fieldType.hasDocValues()) {
            document.add(new IpFieldMapper.InetAddressField(fieldType.name(), address));
        } else if (fieldType.isSearchable()) {
            document.add(new InetAddressPoint(fieldType.name(), address));
        } else if (fieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(fieldType.name(), new BytesRef(InetAddressPoint.encode(address))));
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }
}
