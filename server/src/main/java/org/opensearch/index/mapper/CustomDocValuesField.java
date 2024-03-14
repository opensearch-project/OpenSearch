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

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;

import java.io.Reader;

/**
 * Base class for constructing a custom docvalues type.
 * used for binary, geo, and range fields
 *
 * @opensearch.api
 */
public abstract class CustomDocValuesField implements IndexableField {

    public static final FieldType TYPE = new FieldType();
    static {
        TYPE.setDocValuesType(DocValuesType.BINARY);
        TYPE.setOmitNorms(true);
        TYPE.freeze();
    }

    private final String name;

    protected CustomDocValuesField(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public IndexableFieldType fieldType() {
        return TYPE;
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    @Override
    public Number numericValue() {
        return null;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return null;
    }

}
