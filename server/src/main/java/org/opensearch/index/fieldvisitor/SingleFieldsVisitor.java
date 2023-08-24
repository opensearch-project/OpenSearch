/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Uid;

import java.util.List;

/**
 * {@linkplain StoredFieldVisitor} that loads a single field value.
 *
 * @opensearch.internal
 */
public final class SingleFieldsVisitor extends StoredFieldVisitor {
    private final MappedFieldType field;
    private final List<Object> destination;

    /**
     * Build the field visitor;
     * @param field the name of the field to load
     * @param destination where to put the field's values
     */
    public SingleFieldsVisitor(MappedFieldType field, List<Object> destination) {
        this.field = field;
        this.destination = destination;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (fieldInfo.name.equals(field.name())) {
            return Status.YES;
        }
        /*
         * We can't return Status.STOP here because we could be loading
         * multi-valued fields.
         */
        return Status.NO;
    }

    private void addValue(Object value) {
        destination.add(field.valueForDisplay(value));
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
            addValue(Uid.decodeId(value));
        } else {
            addValue(new BytesRef(value));
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) {
        addValue(value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
        addValue(value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        addValue(value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
        addValue(value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
        addValue(value);
    }
}
