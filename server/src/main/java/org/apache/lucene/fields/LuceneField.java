/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.fields;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

public abstract class LuceneField {

    public abstract void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue);
}
