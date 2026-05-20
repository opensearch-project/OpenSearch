/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hashing;

import org.opensearch.common.hash.FNV1a;
import org.opensearch.test.OpenSearchTestCase;

public class FNV1aTests extends OpenSearchTestCase {

    public void testHash32WithKnownValues() {
        assertEquals(-1114940029532279145L, FNV1a.hash32("1sH3kJO5TyeskNekv2YTbA0segmentsdata"));
        assertEquals(-5313793557685118702L, FNV1a.hash32("1sH3kJO5TyeskNekv2YTbA0segmentsmetadata"));
        assertEquals(-4776941653547780179L, FNV1a.hash32("1sH3kJO5TyeskNekv2YTbA0translogdata"));
        assertEquals(7773876801598345624L, FNV1a.hash32("1sH3kJO5TyeskNekv2YTbA0translogmetadata"));
        assertEquals(3174284101845744576L, FNV1a.hash32("1sH3kJO5TyeskNekv2YTbA0segmentslock_files"));
        assertEquals(875447599647258598L, FNV1a.hash32("hell"));
        assertEquals(1460560186469985451L, FNV1a.hash32("hello"));
        assertEquals(-4959477702557352110L, FNV1a.hash32("hello w"));
        assertEquals(-777130915070571257L, FNV1a.hash32("hello wo"));
        assertEquals(-7887204531510399185L, FNV1a.hash32("hello wor"));
        assertEquals(-782004333700192647L, FNV1a.hash32("hello worl"));
        assertEquals(2168278929747165095L, FNV1a.hash32("hello world"));
        assertEquals(2655121221658607504L, FNV1a.hash32("The quick brown fox jumps over the lazy dog"));
    }

    public void testHash64WithKnownValues() {
        assertEquals(-8975854101357662761L, FNV1a.hash64("1sH3kJO5TyeskNekv2YTbA0segmentsdata"));
        assertEquals(-4380291990281602606L, FNV1a.hash64("1sH3kJO5TyeskNekv2YTbA0segmentsmetadata"));
        assertEquals(-4532418109365814419L, FNV1a.hash64("1sH3kJO5TyeskNekv2YTbA0translogdata"));
        assertEquals(41331743556869080L, FNV1a.hash64("1sH3kJO5TyeskNekv2YTbA0translogmetadata"));
        assertEquals(6170437157231275808L, FNV1a.hash64("1sH3kJO5TyeskNekv2YTbA0segmentslock_files"));
        assertEquals(763638091547294502L, FNV1a.hash64("hell"));
        assertEquals(-6615550055289275125L, FNV1a.hash64("hello"));
        assertEquals(-8428874042178798254L, FNV1a.hash64("hello w"));
        assertEquals(-6323438951910650201L, FNV1a.hash64("hello wo"));
        assertEquals(7042426588567368687L, FNV1a.hash64("hello wor"));
        assertEquals(7273314957493782425L, FNV1a.hash64("hello worl"));
        assertEquals(8618312879776256743L, FNV1a.hash64("hello world"));
        assertEquals(-866459186506731248L, FNV1a.hash64("The quick brown fox jumps over the lazy dog"));
    }

}
