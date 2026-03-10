/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Set;

public class IpLuceneField extends LuceneField {

    /**
     * FieldType that combines dimensional points and sorted set doc values for IP fields.
     * Equivalent to IpFieldMapper.InetAddressField.FIELD_TYPE.
     */
    private static final FieldType INET_ADDRESS_FIELD_TYPE = new FieldType();
    static {
        INET_ADDRESS_FIELD_TYPE.setDimensions(1, InetAddressPoint.BYTES);
        INET_ADDRESS_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_SET); // TODO:: Should we do this?
        INET_ADDRESS_FIELD_TYPE.freeze();
    }

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final InetAddress address = (InetAddress) parseValue;
        boolean indexed = fieldType.isSearchable();
        boolean hasDocValues = fieldType.hasDocValues();
        boolean stored = fieldType.isStored();

        if (indexed && hasDocValues) {
            document.add(new Field(fieldType.name(), new BytesRef(InetAddressPoint.encode(address)), INET_ADDRESS_FIELD_TYPE));
        } else if (indexed) {
            document.add(new InetAddressPoint(fieldType.name(), address));
        } else if (hasDocValues) {
            document.add(new SortedSetDocValuesField(fieldType.name(), new BytesRef(InetAddressPoint.encode(address))));
        }
        if (stored) {
            document.add(new StoredField(fieldType.name(), new BytesRef(InetAddressPoint.encode(address))));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
