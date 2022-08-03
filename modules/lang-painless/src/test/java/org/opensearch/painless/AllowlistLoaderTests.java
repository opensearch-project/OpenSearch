/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistClass;
import org.opensearch.painless.spi.AllowlistLoader;
import org.opensearch.painless.spi.AllowlistMethod;
import org.opensearch.painless.spi.Whitelist;
import org.opensearch.painless.spi.WhitelistClass;
import org.opensearch.painless.spi.WhitelistLoader;
import org.opensearch.painless.spi.WhitelistMethod;
import org.opensearch.painless.spi.annotation.DeprecatedAnnotation;
import org.opensearch.painless.spi.annotation.NoImportAnnotation;
import org.opensearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.HashMap;
import java.util.Map;

public class AllowlistLoaderTests extends ScriptTestCase {
    public void testUnknownAnnotations() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);

        RuntimeException expected = expectThrows(
            RuntimeException.class,
            () -> { AllowlistLoader.loadFromResourceFiles(Allowlist.class, parsers, "org.opensearch.painless.annotation.unknown"); }
        );
        assertEquals("invalid annotation: parser not found for [unknownAnnotation] [@unknownAnnotation]", expected.getCause().getMessage());
        assertEquals(IllegalArgumentException.class, expected.getCause().getClass());

        expected = expectThrows(
            RuntimeException.class,
            () -> {
                AllowlistLoader.loadFromResourceFiles(Allowlist.class, parsers, "org.opensearch.painless.annotation.unknown_with_options");
            }
        );
        assertEquals(
            "invalid annotation: parser not found for [unknownAnootationWithMessage] [@unknownAnootationWithMessage[arg=\"arg value\"]]",
            expected.getCause().getMessage()
        );
        assertEquals(IllegalArgumentException.class, expected.getCause().getClass());
    }

    public void testAnnotations() {
        Map<String, WhitelistAnnotationParser> parsers = new HashMap<>(WhitelistAnnotationParser.BASE_ANNOTATION_PARSERS);
        parsers.put(AnnotationTestObject.TestAnnotation.NAME, AnnotationTestObject.TestAnnotationParser.INSTANCE);
        Allowlist allowlist = AllowlistLoader.loadFromResourceFiles(Allowlist.class, parsers, "org.opensearch.painless.annotation");

        assertEquals(1, allowlist.allowlistClasses.size());

        AllowlistClass allowlistClass = allowlist.allowlistClasses.get(0);

        assertNotNull(allowlistClass.painlessAnnotations.get(NoImportAnnotation.class));
        assertEquals(1, allowlistClass.painlessAnnotations.size());
        assertEquals(3, allowlistClass.allowlistMethods.size());

        int count = 0;

        for (AllowlistMethod allowlistMethod : allowlistClass.allowlistMethods) {
            if ("deprecatedMethod".equals(allowlistMethod.methodName)) {
                assertEquals(
                    "use another method",
                    ((DeprecatedAnnotation) allowlistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).getMessage()
                );
                assertEquals(1, allowlistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedTestMethod".equals(allowlistMethod.methodName)) {
                AnnotationTestObject.TestAnnotation ta = ((AnnotationTestObject.TestAnnotation) allowlistMethod.painlessAnnotations.get(
                    AnnotationTestObject.TestAnnotation.class
                ));
                assertEquals("one", ta.getOne());
                assertEquals("two", ta.getTwo());
                assertEquals("three", ta.getThree());
                assertEquals(1, allowlistMethod.painlessAnnotations.size());
                ++count;
            }

            if ("annotatedMultipleMethod".equals(allowlistMethod.methodName)) {
                assertEquals(
                    "test",
                    ((DeprecatedAnnotation) allowlistMethod.painlessAnnotations.get(DeprecatedAnnotation.class)).getMessage()
                );
                AnnotationTestObject.TestAnnotation ta = ((AnnotationTestObject.TestAnnotation) allowlistMethod.painlessAnnotations.get(
                    AnnotationTestObject.TestAnnotation.class
                ));
                assertEquals("one", ta.getOne());
                assertEquals("two", ta.getTwo());
                assertEquals("three", ta.getThree());
                assertEquals(2, allowlistMethod.painlessAnnotations.size());
                ++count;
            }
        }

        assertEquals(3, count);
    }
}
