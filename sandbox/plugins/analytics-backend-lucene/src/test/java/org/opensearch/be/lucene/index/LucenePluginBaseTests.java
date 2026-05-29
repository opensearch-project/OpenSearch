/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

import static org.opensearch.index.engine.dataformat.FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;

public abstract class LucenePluginBaseTests extends OpenSearchTestCase {

    protected MappedFieldType mockTextField(String name) {
        TextFieldMapper.TextFieldType textFieldType = new TextFieldMapper.TextFieldType(name);
        textFieldType.setCapabilityMap(Map.of(LucenePlugin.DATA_FORMAT, Set.of(FULL_TEXT_SEARCH)));
        return textFieldType;
    }

    protected MappedFieldType mockKeywordField(String name) {
        final FieldType keywordFieldType = new FieldType();
        keywordFieldType.setTokenized(false);
        keywordFieldType.setStored(false);
        keywordFieldType.setOmitNorms(true);
        keywordFieldType.setIndexOptions(IndexOptions.DOCS);
        keywordFieldType.freeze();
        KeywordFieldMapper.KeywordFieldType kft = new KeywordFieldMapper.KeywordFieldType(name, keywordFieldType);
        kft.setCapabilityMap(Map.of(LucenePlugin.DATA_FORMAT, Set.of(FULL_TEXT_SEARCH)));
        return kft;
    }
}
