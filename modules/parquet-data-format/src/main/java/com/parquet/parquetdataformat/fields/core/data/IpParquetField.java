/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields.core.data;

import com.parquet.parquetdataformat.fields.ParquetField;
import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.MappedFieldType;

import java.net.InetAddress;

/**
 * Parquet field implementation for handling IP address data types in OpenSearch documents.
 *
 * <p>This class provides the conversion logic between OpenSearch IP fields and Apache Arrow
 * UTF-8 string vectors for columnar storage in Parquet format. IP address values are encoded
 * using Lucene's {@link InetAddressPoint} encoding and stored using Apache Arrow's 
 * {@link VarCharVector}, which provides efficient variable-length binary storage.</p>
 *
 * <p>This field type corresponds to OpenSearch's {@code ip} field mapping, which is used
 * for storing IPv4 and IPv6 addresses. The IP addresses are internally encoded as binary
 * data using Lucene's point encoding for efficient range queries and storage optimization.</p>
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>{@code
 * IpParquetField ipField = new IpParquetField();
 * ArrowType arrowType = ipField.getArrowType(); // Returns ArrowType.Utf8
 * FieldType fieldType = ipField.getFieldType(); // Returns nullable UTF-8 field type
 * }</pre>
 *
 * @see ParquetField
 * @see VarCharVector
 * @see InetAddressPoint
 * @see ArrowType.Utf8
 * @since 1.0
 */
public class IpParquetField extends ParquetField {

    @Override
    public void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue) {
        VarCharVector varCharVector = (VarCharVector) managedVSR.getVector(mappedFieldType.name());
        int rowIndex = managedVSR.getRowCount();
        final BytesRef bytesRef = new BytesRef(InetAddressPoint.encode((InetAddress) parseValue));
        varCharVector.setSafe(rowIndex, bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    @Override
    public ArrowType getArrowType() {
        return new ArrowType.Utf8();
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.nullable(getArrowType());
    }
}
