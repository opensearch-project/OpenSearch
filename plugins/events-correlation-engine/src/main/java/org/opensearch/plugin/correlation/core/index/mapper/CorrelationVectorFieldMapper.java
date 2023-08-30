/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.core.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.Explicit;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.plugin.correlation.core.index.CorrelationParamsContext;
import org.opensearch.plugin.correlation.core.index.VectorField;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import static org.apache.lucene.index.FloatVectorValues.MAX_DIMENSIONS;

/**
 * Field mapper for the correlation vector type
 *
 * @opensearch.internal
 */
public class CorrelationVectorFieldMapper extends VectorFieldMapper {

    private static final int LUCENE_MAX_DIMENSION = MAX_DIMENSIONS;

    private final FieldType vectorFieldType;

    /**
     * Parameterized ctor for CorrelationVectorFieldMapper
     * @param input Object containing name of the field, type and other details.
     */
    public CorrelationVectorFieldMapper(final CreateLuceneFieldMapperInput input) {
        super(
            input.getName(),
            input.getMappedFieldType(),
            input.getMultiFields(),
            input.getCopyTo(),
            input.getIgnoreMalformed(),
            input.isStored(),
            input.isHasDocValues()
        );

        this.correlationParams = input.getCorrelationParams();
        final VectorSimilarityFunction vectorSimilarityFunction = this.correlationParams.getSimilarityFunction();

        final int dimension = input.getMappedFieldType().getDimension();
        if (dimension > LUCENE_MAX_DIMENSION) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Dimension value cannot be greater than [%s] but got [%s] for vector [%s]",
                    LUCENE_MAX_DIMENSION,
                    dimension,
                    input.getName()
                )
            );
        }

        this.fieldType = KnnFloatVectorField.createFieldType(dimension, vectorSimilarityFunction);

        if (this.hasDocValues) {
            this.vectorFieldType = buildDocValuesFieldType();
        } else {
            this.vectorFieldType = null;
        }
    }

    private static FieldType buildDocValuesFieldType() {
        FieldType field = new FieldType();
        field.setDocValuesType(DocValuesType.BINARY);
        field.freeze();
        return field;
    }

    @Override
    protected void parseCreateField(ParseContext context, int dimension) throws IOException {
        Optional<float[]> arrayOptional = getFloatsFromContext(context, dimension);

        if (arrayOptional.isEmpty()) {
            return;
        }
        final float[] array = arrayOptional.get();

        KnnFloatVectorField point = new KnnFloatVectorField(name(), array, fieldType);

        context.doc().add(point);
        if (fieldType.stored()) {
            context.doc().add(new StoredField(name(), point.toString()));
        }
        if (hasDocValues && vectorFieldType != null) {
            context.doc().add(new VectorField(name(), array, vectorFieldType));
        }
        context.path().remove();
    }

    static class CreateLuceneFieldMapperInput {
        String name;

        CorrelationVectorFieldType mappedFieldType;

        FieldMapper.MultiFields multiFields;

        FieldMapper.CopyTo copyTo;

        Explicit<Boolean> ignoreMalformed;
        boolean stored;
        boolean hasDocValues;

        CorrelationParamsContext correlationParams;

        public CreateLuceneFieldMapperInput(
            String name,
            CorrelationVectorFieldType mappedFieldType,
            FieldMapper.MultiFields multiFields,
            FieldMapper.CopyTo copyTo,
            Explicit<Boolean> ignoreMalformed,
            boolean stored,
            boolean hasDocValues,
            CorrelationParamsContext correlationParams
        ) {
            this.name = name;
            this.mappedFieldType = mappedFieldType;
            this.multiFields = multiFields;
            this.copyTo = copyTo;
            this.ignoreMalformed = ignoreMalformed;
            this.stored = stored;
            this.hasDocValues = hasDocValues;
            this.correlationParams = correlationParams;
        }

        public String getName() {
            return name;
        }

        public CorrelationVectorFieldType getMappedFieldType() {
            return mappedFieldType;
        }

        public FieldMapper.MultiFields getMultiFields() {
            return multiFields;
        }

        public FieldMapper.CopyTo getCopyTo() {
            return copyTo;
        }

        public Explicit<Boolean> getIgnoreMalformed() {
            return ignoreMalformed;
        }

        public boolean isStored() {
            return stored;
        }

        public boolean isHasDocValues() {
            return hasDocValues;
        }

        public CorrelationParamsContext getCorrelationParams() {
            return correlationParams;
        }
    }
}
