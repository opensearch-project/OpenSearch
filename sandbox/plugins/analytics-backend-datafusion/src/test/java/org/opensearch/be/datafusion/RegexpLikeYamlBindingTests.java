/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import io.substrait.extension.SimpleExtension;

/** Pins the {@code regexp_like} substrait extension binding so a yaml edit
 *  can't silently break the PPL {@code REGEXP} -> DataFusion lowering. */
public class RegexpLikeYamlBindingTests extends OpenSearchTestCase {

    private static List<SimpleExtension.ScalarFunctionVariant> regexpLikeVariants() throws Exception {
        Thread t = Thread.currentThread();
        ClassLoader prev = t.getContextClassLoader();
        try {
            t.setContextClassLoader(RegexpLikeYamlBindingTests.class.getClassLoader());
            SimpleExtension.ExtensionCollection scalars = SimpleExtension.load(List.of("/opensearch_scalar_functions.yaml"));
            return scalars.scalarFunctions().stream().filter(v -> "regexp_like".equals(v.name())).collect(Collectors.toList());
        } finally {
            t.setContextClassLoader(prev);
        }
    }

    public void testRegexpLikeIsRegistered() throws Exception {
        assertFalse("regexp_like must be registered in opensearch_scalar_functions.yaml", regexpLikeVariants().isEmpty());
    }

    public void testRegexpLikeHasStrAndVcharImpls() throws Exception {
        // Both keys are required: isthmus emits str_str for STRING operands and vchar_vchar for fixed-width VARCHAR.
        Set<String> keys = regexpLikeVariants().stream().map(SimpleExtension.ScalarFunctionVariant::key).collect(Collectors.toSet());
        assertTrue("missing regexp_like:str_str impl, found keys=" + keys, keys.contains("regexp_like:str_str"));
        assertTrue("missing regexp_like:vchar_vchar impl, found keys=" + keys, keys.contains("regexp_like:vchar_vchar"));
    }

    public void testRegexpLikeImplsAreBinaryAndReturnBoolean() throws Exception {
        for (SimpleExtension.ScalarFunctionVariant v : regexpLikeVariants()) {
            assertEquals("regexp_like " + v.key() + " arity", 2, v.args().size());
            String rt = v.returnType().toString().toLowerCase(Locale.ROOT);
            assertTrue("regexp_like " + v.key() + " return type must be boolean (saw " + v.returnType() + ")", rt.contains("bool"));
        }
    }
}
