/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class WildcardFieldMapperTest extends MapperTestCase {

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "wildcard");
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.value("value");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("normalizer", b -> b.field("normalizer", "lowercase"));
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", false));
        checker.registerConflictCheck("null_value", b -> b.field("null_value", "foo"));
        checker.registerUpdateCheck(b -> b.field("ignore_above", 256), m -> assertEquals(256, ((WildcardFieldMapper) m).ignoreAbove()));
    }

    public void testTokenizer() throws IOException {
        List<String> terms = new ArrayList<>();
        try (Tokenizer tokenizer = new WildcardFieldMapper.WildcardFieldTokenizer()) {
            tokenizer.setReader(new StringReader("pickle"));
            tokenizer.reset();
            CharTermAttribute charTermAttribute = tokenizer.getAttribute(CharTermAttribute.class);
            while (tokenizer.incrementToken()) {
                terms.add(charTermAttribute.toString());
            }
        }
        assertEquals(
            List.of(
                WildcardFieldTypeTest.prefixAnchored("p"),
                WildcardFieldTypeTest.prefixAnchored("pi"),
                "p",
                "pi",
                "pic",
                "i",
                "ic",
                "ick",
                "c",
                "ck",
                "ckl",
                "k",
                "kl",
                "kle",
                "l",
                "le",
                WildcardFieldTypeTest.suffixAnchored("le"),
                "e",
                WildcardFieldTypeTest.suffixAnchored("e")
            ),
            terms
        );
    }
}
