/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.inject.internal;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Type;

public class MoreTypesTests extends OpenSearchTestCase {

    public void testHashCode() {
        assertEquals(0, MoreTypes.hashCode(null));
        assertEquals(String.class.hashCode(), MoreTypes.hashCode(String.class));

        MoreTypes.ParameterizedTypeImpl paramType = new MoreTypes.ParameterizedTypeImpl(null, java.util.List.class, String.class);
        int expectedParamHash = java.util.Arrays.hashCode(new Type[] { String.class }) ^ java.util.List.class.hashCode();
        assertEquals(expectedParamHash, MoreTypes.hashCode(paramType));

        MoreTypes.GenericArrayTypeImpl arrayType = new MoreTypes.GenericArrayTypeImpl(String.class);
        assertEquals(String.class.hashCode(), MoreTypes.hashCode(arrayType));

        MoreTypes.WildcardTypeImpl wildcardType = new MoreTypes.WildcardTypeImpl(new Type[] { String.class }, new Type[] {});
        int expectedWildcardHash = java.util.Arrays.hashCode(new Type[] {}) ^ java.util.Arrays.hashCode(new Type[] { String.class });
        assertEquals(expectedWildcardHash, MoreTypes.hashCode(wildcardType));
    }

    public void testToString() {
        assertEquals("java.lang.String", MoreTypes.toString(String.class));

        MoreTypes.ParameterizedTypeImpl paramType = new MoreTypes.ParameterizedTypeImpl(null, java.util.List.class, String.class);
        assertEquals("java.util.List<java.lang.String>", MoreTypes.toString(paramType));

        MoreTypes.GenericArrayTypeImpl arrayType = new MoreTypes.GenericArrayTypeImpl(String.class);
        assertEquals("java.lang.String[]", MoreTypes.toString(arrayType));

        MoreTypes.WildcardTypeImpl unboundedWildcard = new MoreTypes.WildcardTypeImpl(
            new java.lang.reflect.Type[] { Object.class },
            new java.lang.reflect.Type[] {}
        );
        assertEquals("?", MoreTypes.toString(unboundedWildcard));

        MoreTypes.WildcardTypeImpl upperBoundedWildcard = new MoreTypes.WildcardTypeImpl(
            new java.lang.reflect.Type[] { String.class },
            new java.lang.reflect.Type[] {}
        );
        assertEquals("? extends java.lang.String", MoreTypes.toString(upperBoundedWildcard));

        MoreTypes.WildcardTypeImpl lowerBoundedWildcard = new MoreTypes.WildcardTypeImpl(
            new java.lang.reflect.Type[] { Object.class },
            new java.lang.reflect.Type[] { String.class }
        );
        assertEquals("? super java.lang.String", MoreTypes.toString(lowerBoundedWildcard));

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> { MoreTypes.toString((Type) null); }
        );
        assertEquals("Unsupported wildcard type [null]", exception.getMessage());
    }

    public void testGetRawType() {
        assertEquals(String.class, MoreTypes.getRawType(String.class));

        MoreTypes.ParameterizedTypeImpl paramType = new MoreTypes.ParameterizedTypeImpl(null, java.util.List.class, String.class);
        assertEquals(java.util.List.class, MoreTypes.getRawType(paramType));

        MoreTypes.GenericArrayTypeImpl arrayType = new MoreTypes.GenericArrayTypeImpl(String.class);
        assertEquals(Object[].class, MoreTypes.getRawType(arrayType));

        java.lang.reflect.TypeVariable<?> typeVar = java.util.List.class.getTypeParameters()[0];
        assertEquals(Object.class, MoreTypes.getRawType(typeVar));

        MoreTypes.WildcardTypeImpl wildcardType = new MoreTypes.WildcardTypeImpl(new Type[] { String.class }, new Type[] {});
        IllegalArgumentException wildcardException = expectThrows(IllegalArgumentException.class, () -> MoreTypes.getRawType(wildcardType));
        assertTrue(wildcardException.getMessage().contains("Expected a Class, ParameterizedType, or GenericArrayType"));

        IllegalArgumentException nullException = expectThrows(IllegalArgumentException.class, () -> MoreTypes.getRawType(null));
        assertTrue(nullException.getMessage().contains("Unsupported type [null]"));
    }
}
