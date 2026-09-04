/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields.core.data.text;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.vsr.ManagedVSR;

import java.net.InetAddress;
import java.util.Set;

/**
 * Parquet field for IP address values stored as binary using {@link VarBinaryVector}.
 */
public class IpParquetField extends ParquetField {

    /** Creates a new IpParquetField. */
    public IpParquetField() {}

    @Override
    protected void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        addToVector(managedVSR.getVector(mappedFieldType.name()), managedVSR.getRowCount(), parseValue);
    }

    @Override
    protected void addToVector(FieldVector vector, int index, Object parseValue) {
        BytesRef bytesRef = new BytesRef(InetAddressPoint.encode((InetAddress) parseValue));
        ((VarBinaryVector) vector).setSafe(index, bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    @Override
    public boolean supportsMultiValue() {
        return true;
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Binary();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }

    @Override
    public Set<FieldTypeCapabilities.Capability> supportedCapabilities() {
        return Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE, FieldTypeCapabilities.Capability.BLOOM_FILTER);
    }
}
