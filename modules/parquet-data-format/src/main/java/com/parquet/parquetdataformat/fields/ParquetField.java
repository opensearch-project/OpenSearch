/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.fields;

import com.parquet.parquetdataformat.vsr.ManagedVSR;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Objects;

/**
 * Abstract base class for all Parquet field implementations that handle the conversion
 * between OpenSearch field types and Apache Arrow/Parquet data structures.
 *
 * <p>This class defines the contract for field-specific operations including:
 * <ul>
 *   <li>Adding field data to vector groups for columnar storage</li>
 *   <li>Creating field instances with proper type validation</li>
 *   <li>Providing Arrow type definitions for schema generation</li>
 *   <li>Generating field type metadata for Arrow schemas</li>
 * </ul>
 *
 * <p>Implementations of this class should be thread-safe and stateless, as they
 * may be shared across multiple processing contexts.</p>
 *
 * @see ArrowFieldRegistry
 * @see ManagedVSR
 */
public abstract class ParquetField {

    /**
     * Adds the parsed field value to the appropriate vector group within the managed VSR.
     * This method is responsible for the actual data conversion and storage in the
     * columnar format specific to each field type.
     *
     * <p>Implementations must handle null values appropriately and ensure type safety
     * when casting the parseValue to the expected type.</p>
     *
     * @param mappedFieldType the OpenSearch field type metadata containing field configuration
     * @param managedVSR the managed vector schema root for columnar data storage
     * @param parseValue the parsed field value to be stored, may be null
     * @throws IllegalArgumentException if any parameter is invalid for this field type
     * @throws ClassCastException if parseValue cannot be cast to the expected type
     */
    protected abstract void addToGroup(MappedFieldType mappedFieldType, ManagedVSR managedVSR, Object parseValue);

    /**
     * Creates and processes a field entry if the field type supports columnar storage.
     * This method serves as the main entry point for field processing and includes
     * validation logic to ensure only columnar fields are processed.
     *
     * <p>The method performs the following operations:
     * <ol>
     *   <li>Validates input parameters</li>
     *   <li>Checks if the field supports columnar storage</li>
     *   <li>Delegates to {@link #addToGroup} for actual data processing</li>
     * </ol>
     *
     * @param mappedFieldType the OpenSearch field type metadata, must not be null
     * @param managedVSR the managed vector schema root, must not be null
     * @param parseValue the parsed field value to be processed, may be null
     * @throws IllegalArgumentException if mappedFieldType or managedVSR is null
     */
    public final void createField(final MappedFieldType mappedFieldType,
                                  final ManagedVSR managedVSR,
                                  final Object parseValue) {
        Objects.requireNonNull(mappedFieldType, "MappedFieldType cannot be null");
        Objects.requireNonNull(managedVSR, "ManagedVSR cannot be null");

        if (mappedFieldType.isColumnar()) {
            // TODO: support dynamic mapping update
            // for now ignore the field
            if (managedVSR.getVector(mappedFieldType.name()) != null) {
                addToGroup(mappedFieldType, managedVSR, parseValue);
            }
        }
    }

    /**
     * Returns the Apache Arrow type definition for this field.
     * This type definition is used for schema generation and data type validation
     * in the Arrow/Parquet ecosystem.
     *
     * <p>The returned ArrowType should be consistent across all instances of the
     * same field implementation and should accurately represent the data type
     * that will be stored.</p>
     *
     * @return the Arrow type definition for this field, never null
     */
    public abstract ArrowType getArrowType();

    /**
     * Returns the Apache Arrow field type with metadata for schema generation.
     * This includes the base Arrow type along with additional metadata such as
     * nullability constraints and custom properties.
     *
     * <p>The returned FieldType is used when constructing Arrow schemas and
     * should include appropriate nullability settings based on the field's
     * characteristics.</p>
     *
     * @return the complete field type definition including metadata, never null
     */
    public abstract FieldType getFieldType();

    /**
     * Provides a string representation of this ParquetField for debugging purposes.
     * The default implementation includes the class name and Arrow type information.
     *
     * @return a string representation of this field
     */
    @Override
    public String toString() {
        return String.format("%s{arrowType=%s}",
            this.getClass().getSimpleName(),
            getArrowType());
    }
}
