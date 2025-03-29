/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FilterFieldTypeTest extends OpenSearchTestCase {

    private static final class MethodSignature {
        private final String name;
        private final Class<?> returnType;
        private final Class<?>[] parameterTypes;

        public MethodSignature(String name, Class<?> returnType, Class<?>[] parameterTypes) {
            this.name = name;
            this.returnType = returnType;
            this.parameterTypes = parameterTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            MethodSignature that = (MethodSignature) o;
            return Objects.equals(name, that.name)
                && Objects.equals(returnType, that.returnType)
                && Objects.deepEquals(parameterTypes, that.parameterTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, returnType, Arrays.hashCode(parameterTypes));
        }
    }

    private static final Set<MethodSignature> EXCLUDED_SIGNATURES = Set.of(new MethodSignature("typeName", String.class, new Class<?>[0]));

    public void testAllMethodsDelegated() {
        Method[] mappedFieldTypeMethods = MappedFieldType.class.getMethods();
        Method[] filterFieldTypeMethods = FilterFieldType.class.getMethods();

        Set<MethodSignature> mappedFieldTypeMethodSignatures = new HashSet<>();
        for (Method method : mappedFieldTypeMethods) {
            if (method.getDeclaringClass() == MappedFieldType.class
                && Modifier.isFinal(method.getModifiers()) == false
                && Modifier.isStatic(method.getModifiers()) == false) {
                mappedFieldTypeMethodSignatures.add(
                    new MethodSignature(method.getName(), method.getReturnType(), method.getParameterTypes())
                );
            }
        }

        Set<MethodSignature> filterFieldTypeMethodSignatures = new HashSet<>();
        for (Method method : filterFieldTypeMethods) {
            if (method.getDeclaringClass() == FilterFieldType.class) {
                filterFieldTypeMethodSignatures.add(
                    new MethodSignature(method.getName(), method.getReturnType(), method.getParameterTypes())
                );
            }
        }
        for (MethodSignature methodSignature : mappedFieldTypeMethodSignatures) {
            if (filterFieldTypeMethodSignatures.contains(methodSignature)) {
                assertFalse(
                    "Method " + methodSignature.name + " should NOT be implemented in " + FilterFieldType.class.getSimpleName(),
                    EXCLUDED_SIGNATURES.contains(methodSignature)
                );
            } else {
                assertTrue(
                    "Method " + methodSignature.name + " should be implemented in " + FilterFieldType.class.getSimpleName(),
                    EXCLUDED_SIGNATURES.contains(methodSignature)
                );
            }
        }
    }

}
