/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject.internal;

import org.opensearch.common.inject.ConfigurationException;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.spi.Message;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.GenericDeclaration;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableMap;

/**
 * Static methods for working with types that we aren't publishing in the
 * public {@code Types} API.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
public class MoreTypes {

    public static final Type[] EMPTY_TYPE_ARRAY = new Type[] {};

    private MoreTypes() {}

    private static final Map<TypeLiteral<?>, TypeLiteral<?>> PRIMITIVE_TO_WRAPPER;
    static {
        Map<TypeLiteral<?>, TypeLiteral<?>> primitiveToWrapper = new HashMap<>();
        primitiveToWrapper.put(TypeLiteral.get(boolean.class), TypeLiteral.get(Boolean.class));
        primitiveToWrapper.put(TypeLiteral.get(byte.class), TypeLiteral.get(Byte.class));
        primitiveToWrapper.put(TypeLiteral.get(short.class), TypeLiteral.get(Short.class));
        primitiveToWrapper.put(TypeLiteral.get(int.class), TypeLiteral.get(Integer.class));
        primitiveToWrapper.put(TypeLiteral.get(long.class), TypeLiteral.get(Long.class));
        primitiveToWrapper.put(TypeLiteral.get(float.class), TypeLiteral.get(Float.class));
        primitiveToWrapper.put(TypeLiteral.get(double.class), TypeLiteral.get(Double.class));
        primitiveToWrapper.put(TypeLiteral.get(char.class), TypeLiteral.get(Character.class));
        primitiveToWrapper.put(TypeLiteral.get(void.class), TypeLiteral.get(Void.class));
        PRIMITIVE_TO_WRAPPER = unmodifiableMap(primitiveToWrapper);
    }

    /**
     * Returns an equivalent type that's safe for use in a key. The returned type will be free of
     * primitive types. Type literals of primitives will return the corresponding wrapper types.
     *
     * @throws ConfigurationException if {@code type} contains a type variable
     */
    public static <T> TypeLiteral<T> makeKeySafe(final TypeLiteral<T> type) {
        if (!isFullySpecified(type.getType())) {
            final String message = type + " cannot be used as a key; It is not fully specified.";
            throw new ConfigurationException(singleton(new Message(message)));
        }

        @SuppressWarnings("unchecked")
        final TypeLiteral<T> wrappedPrimitives = (TypeLiteral<T>) PRIMITIVE_TO_WRAPPER.get(type);
        return wrappedPrimitives != null ? wrappedPrimitives : type;
    }

    /**
     * Returns true if {@code type} is free from type variables.
     */
    private static boolean isFullySpecified(final Type type) {
        if (type instanceof Class) {
            return true;

        } else if (type instanceof CompositeType) {
            return ((CompositeType) type).isFullySpecified();

        } else if (type instanceof TypeVariable) {
            return false;

        } else {
            return ((CompositeType) canonicalize(type)).isFullySpecified();
        }
    }

    /**
     * Returns a type that is functionally equal but not necessarily equal
     * according to {@link Object#equals(Object) Object.equals()}.
     */
    public static Type canonicalize(final Type type) {
        if (type instanceof ParameterizedTypeImpl || type instanceof GenericArrayTypeImpl || type instanceof WildcardTypeImpl) {
            return type;

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType p = (ParameterizedType) type;
            return new ParameterizedTypeImpl(p.getOwnerType(), p.getRawType(), p.getActualTypeArguments());

        } else if (type instanceof GenericArrayType) {
            final GenericArrayType g = (GenericArrayType) type;
            return new GenericArrayTypeImpl(g.getGenericComponentType());

        } else if (type instanceof Class && ((Class<?>) type).isArray()) {
            final Class<?> c = (Class<?>) type;
            return new GenericArrayTypeImpl(c.getComponentType());

        } else if (type instanceof WildcardType) {
            WildcardType w = (WildcardType) type;
            return new WildcardTypeImpl(w.getUpperBounds(), w.getLowerBounds());

        } else {
            // type is either serializable as-is or unsupported
            return type;
        }
    }

    public static Class<?> getRawType(final Type type) {
        if (type instanceof Class<?>) {
            // type is a normal class.
            return (Class<?>) type;

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;

            // I'm not exactly sure why getRawType() returns Type instead of Class.
            // Neal isn't either but suspects some pathological case related
            // to nested classes exists.
            final Type rawType = parameterizedType.getRawType();
            if (!(rawType instanceof Class)) {
                throw new IllegalArgumentException("Expected a Class, but <" + type + "> is of type " + type.getClass().getName());
            }
            return (Class<?>) rawType;

        } else if (type instanceof GenericArrayType) {
            // TODO: Is this sufficient?
            return Object[].class;

        } else if (type instanceof TypeVariable) {
            // we could use the variable's bounds, but that'll won't work if there are multiple.
            // having a raw type that's more general than necessary is okay
            return Object.class;

        } else {
            throw new IllegalArgumentException(
                "Expected a Class, ParameterizedType, or " + "GenericArrayType, but <" + type + "> is of type " + type.getClass().getName()
            );
        }
    }

    /**
     * Returns true if {@code a} and {@code b} are equal.
     */
    public static boolean equals(final Type a, final Type b) {
        if (a == b) {
            // also handles (a == null && b == null)
            return true;

        } else if (a instanceof Class) {
            // Class already specifies equals().
            return a.equals(b);

        } else if (a instanceof ParameterizedType) {
            if (!(b instanceof ParameterizedType)) {
                return false;
            }

            // TODO: save a .clone() call
            final ParameterizedType pa = (ParameterizedType) a;
            final ParameterizedType pb = (ParameterizedType) b;
            return Objects.equals(pa.getOwnerType(), pb.getOwnerType())
                && pa.getRawType().equals(pb.getRawType())
                && Arrays.equals(pa.getActualTypeArguments(), pb.getActualTypeArguments());

        } else if (a instanceof GenericArrayType) {
            if (!(b instanceof GenericArrayType)) {
                return false;
            }

            final GenericArrayType ga = (GenericArrayType) a;
            final GenericArrayType gb = (GenericArrayType) b;
            return equals(ga.getGenericComponentType(), gb.getGenericComponentType());

        } else if (a instanceof WildcardType) {
            if (!(b instanceof WildcardType)) {
                return false;
            }

            final WildcardType wa = (WildcardType) a;
            final WildcardType wb = (WildcardType) b;
            return Arrays.equals(wa.getUpperBounds(), wb.getUpperBounds()) && Arrays.equals(wa.getLowerBounds(), wb.getLowerBounds());

        } else if (a instanceof TypeVariable) {
            if (!(b instanceof TypeVariable)) {
                return false;
            }
            final TypeVariable<?> va = (TypeVariable) a;
            final TypeVariable<?> vb = (TypeVariable) b;
            return va.getGenericDeclaration() == vb.getGenericDeclaration() && va.getName().equals(vb.getName());

        } else {
            // This isn't a type we support. Could be a generic array type, wildcard type, etc.
            return false;
        }
    }

    /**
     * Returns the hashCode of {@code type}.
     */
    public static int hashCode(final Type type) {
        if (type instanceof Class) {
            // Class specifies hashCode().
            return type.hashCode();

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType p = (ParameterizedType) type;
            return Arrays.hashCode(p.getActualTypeArguments()) ^ p.getRawType().hashCode() ^ hashCodeOrZero(p.getOwnerType());

        } else if (type instanceof GenericArrayType) {
            return hashCode(((GenericArrayType) type).getGenericComponentType());

        } else if (type instanceof WildcardType) {
            final WildcardType w = (WildcardType) type;
            return Arrays.hashCode(w.getLowerBounds()) ^ Arrays.hashCode(w.getUpperBounds());

        } else {
            // This isn't a type we support. Probably a type variable
            return hashCodeOrZero(type);
        }
    }

    private static int hashCodeOrZero(final Object o) {
        return o != null ? o.hashCode() : 0;
    }

    public static String toString(final Type type) {
        if (type instanceof Class<?>) {
            return ((Class) type).getName();

        } else if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;
            final Type[] arguments = parameterizedType.getActualTypeArguments();
            final Type ownerType = parameterizedType.getOwnerType();
            final StringBuilder stringBuilder = new StringBuilder();
            if (ownerType != null) {
                stringBuilder.append(toString(ownerType)).append(".");
            }
            stringBuilder.append(toString(parameterizedType.getRawType()));
            if (arguments.length > 0) {
                stringBuilder.append("<").append(toString(arguments[0]));
                for (int i = 1; i < arguments.length; i++) {
                    stringBuilder.append(", ").append(toString(arguments[i]));
                }
            }
            return stringBuilder.append(">").toString();

        } else if (type instanceof GenericArrayType) {
            return toString(((GenericArrayType) type).getGenericComponentType()) + "[]";

        } else if (type instanceof WildcardType) {
            final WildcardType wildcardType = (WildcardType) type;
            final Type[] lowerBounds = wildcardType.getLowerBounds();
            final Type[] upperBounds = wildcardType.getUpperBounds();

            if (upperBounds.length != 1 || lowerBounds.length > 1) {
                throw new UnsupportedOperationException("Unsupported wildcard type " + type);
            }

            if (lowerBounds.length == 1) {
                if (upperBounds[0] != Object.class) {
                    throw new UnsupportedOperationException("Unsupported wildcard type " + type);
                }
                return "? super " + toString(lowerBounds[0]);
            } else if (upperBounds[0] == Object.class) {
                return "?";
            } else {
                return "? extends " + toString(upperBounds[0]);
            }

        } else {
            return type.toString();
        }
    }

    /**
     * Returns {@code Field.class}, {@code Method.class} or {@code Constructor.class}.
     */
    public static Class<? extends Member> memberType(final Member member) {
        Objects.requireNonNull(member, "member");

        if (member instanceof Field) {
            return Field.class;

        } else if (member instanceof Method) {
            return Method.class;

        } else if (member instanceof Constructor) {
            return Constructor.class;

        } else {
            throw new IllegalArgumentException("Unsupported implementation class for Member, " + member.getClass());
        }
    }

    /**
     * Formats a member as concise string, such as {@code java.util.ArrayList.size},
     * {@code java.util.ArrayList<init>()} or {@code java.util.List.remove()}.
     */
    public static String toString(final Member member) {
        final Class<? extends Member> memberType = memberType(member);

        if (memberType == Method.class) {
            return member.getDeclaringClass().getName() + "." + member.getName() + "()";
        } else if (memberType == Field.class) {
            return member.getDeclaringClass().getName() + "." + member.getName();
        } else if (memberType == Constructor.class) {
            return member.getDeclaringClass().getName() + ".<init>()";
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Returns the generic supertype for {@code supertype}. For example, given a class {@code
     * IntegerSet}, the result for when supertype is {@code Set.class} is {@code Set<Integer>} and the
     * result when the supertype is {@code Collection.class} is {@code Collection<Integer>}.
     */
    public static Type getGenericSupertype(final Type type, Class<?> rawType, final Class<?> toResolve) {
        if (toResolve == rawType) {
            return type;
        }

        // we skip searching through interfaces if unknown is an interface
        if (toResolve.isInterface()) {
            final Class[] interfaces = rawType.getInterfaces();
            for (int i = 0, length = interfaces.length; i < length; i++) {
                if (interfaces[i] == toResolve) {
                    return rawType.getGenericInterfaces()[i];
                } else if (toResolve.isAssignableFrom(interfaces[i])) {
                    return getGenericSupertype(rawType.getGenericInterfaces()[i], interfaces[i], toResolve);
                }
            }
        }

        // check our supertypes
        if (!rawType.isInterface()) {
            while (rawType != Object.class) {
                final Class<?> rawSupertype = rawType.getSuperclass();
                if (rawSupertype == toResolve) {
                    return rawType.getGenericSuperclass();
                } else if (toResolve.isAssignableFrom(rawSupertype)) {
                    return getGenericSupertype(rawType.getGenericSuperclass(), rawSupertype, toResolve);
                }
                rawType = rawSupertype;
            }
        }

        // we can't resolve this further
        return toResolve;
    }

    public static Type resolveTypeVariable(final Type type, final Class<?> rawType, final TypeVariable unknown) {
        final Class<?> declaredByRaw = declaringClassOf(unknown);

        // we can't reduce this further
        if (declaredByRaw == null) {
            return unknown;
        }

        final Type declaredBy = getGenericSupertype(type, rawType, declaredByRaw);
        if (declaredBy instanceof ParameterizedType) {
            int index = indexOf(declaredByRaw.getTypeParameters(), unknown);
            return ((ParameterizedType) declaredBy).getActualTypeArguments()[index];
        }

        return unknown;
    }

    private static int indexOf(final Object[] array, final Object toFind) {
        for (int i = 0; i < array.length; i++) {
            if (toFind.equals(array[i])) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Returns the declaring class of {@code typeVariable}, or {@code null} if it was not declared by
     * a class.
     */
    private static Class<?> declaringClassOf(final TypeVariable typeVariable) {
        final GenericDeclaration genericDeclaration = typeVariable.getGenericDeclaration();
        return genericDeclaration instanceof Class ? (Class<?>) genericDeclaration : null;
    }

    /**
     * Implementation for a parameterized type
     *
     * @opensearch.internal
     */
    public static class ParameterizedTypeImpl implements ParameterizedType, CompositeType {
        private final Type ownerType;
        private final Type rawType;
        private final Type[] typeArguments;

        public ParameterizedTypeImpl(final Type ownerType, final Type rawType, final Type... typeArguments) {
            // require an owner type if the raw type needs it
            if (rawType instanceof Class<?>) {
                Class rawTypeAsClass = (Class) rawType;
                if (ownerType == null && rawTypeAsClass.getEnclosingClass() != null) {
                    throw new IllegalArgumentException("No owner type for enclosed " + rawType);
                }
                if (ownerType != null && rawTypeAsClass.getEnclosingClass() == null) {
                    throw new IllegalArgumentException("Owner type for unenclosed " + rawType);
                }

            }

            this.ownerType = ownerType == null ? null : canonicalize(ownerType);
            this.rawType = canonicalize(rawType);
            this.typeArguments = typeArguments.clone();
            for (int t = 0; t < this.typeArguments.length; t++) {
                Objects.requireNonNull(this.typeArguments[t], "type parameter");
                checkNotPrimitive(this.typeArguments[t], "type parameters");
                this.typeArguments[t] = canonicalize(this.typeArguments[t]);
            }
        }

        @Override
        public Type[] getActualTypeArguments() {
            return typeArguments.clone();
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            return ownerType;
        }

        @Override
        public boolean isFullySpecified() {
            if (ownerType != null && !MoreTypes.isFullySpecified(ownerType)) {
                return false;
            }

            if (!MoreTypes.isFullySpecified(rawType)) {
                return false;
            }

            for (final Type type : typeArguments) {
                if (!MoreTypes.isFullySpecified(type)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ParameterizedType && MoreTypes.equals(this, (ParameterizedType) other);
        }

        @Override
        public int hashCode() {
            return MoreTypes.hashCode(this);
        }

        @Override
        public String toString() {
            return MoreTypes.toString(this);
        }
    }

    /**
     * Implementation for a generic array
     *
     * @opensearch.internal
     */
    public static class GenericArrayTypeImpl implements GenericArrayType, CompositeType {
        private final Type componentType;

        public GenericArrayTypeImpl(final Type componentType) {
            this.componentType = canonicalize(componentType);
        }

        @Override
        public Type getGenericComponentType() {
            return componentType;
        }

        @Override
        public boolean isFullySpecified() {
            return MoreTypes.isFullySpecified(componentType);
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof GenericArrayType && MoreTypes.equals(this, (GenericArrayType) o);
        }

        @Override
        public int hashCode() {
            return MoreTypes.hashCode(this);
        }

        @Override
        public String toString() {
            return MoreTypes.toString(this);
        }
    }

    /**
     * The WildcardType interface supports multiple upper bounds and multiple
     * lower bounds. We only support what the Java 6 language needs - at most one
     * bound. If a lower bound is set, the upper bound must be Object.class.
     *
     * @opensearch.internal
     */
    public static class WildcardTypeImpl implements WildcardType, CompositeType {
        private final Type upperBound;
        private final Type lowerBound;

        public WildcardTypeImpl(final Type[] upperBounds, final Type[] lowerBounds) {
            if (lowerBounds.length > 1) {
                throw new IllegalArgumentException("Must have at most one lower bound.");
            }
            if (upperBounds.length != 1) {
                throw new IllegalArgumentException("Must have exactly one upper bound.");
            }
            if (lowerBounds.length == 1) {
                Objects.requireNonNull(lowerBounds[0], "lowerBound");
                checkNotPrimitive(lowerBounds[0], "wildcard bounds");
                if (upperBounds[0] != Object.class) {
                    throw new IllegalArgumentException("bounded both ways");
                }
                this.lowerBound = canonicalize(lowerBounds[0]);
                this.upperBound = Object.class;

            } else {
                Objects.requireNonNull(upperBounds[0], "upperBound");
                checkNotPrimitive(upperBounds[0], "wildcard bounds");
                this.lowerBound = null;
                this.upperBound = canonicalize(upperBounds[0]);
            }
        }

        @Override
        public Type[] getUpperBounds() {
            return new Type[] { upperBound };
        }

        @Override
        public Type[] getLowerBounds() {
            return lowerBound != null ? new Type[] { lowerBound } : EMPTY_TYPE_ARRAY;
        }

        @Override
        public boolean isFullySpecified() {
            return MoreTypes.isFullySpecified(upperBound) && (lowerBound == null || MoreTypes.isFullySpecified(lowerBound));
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof WildcardType && MoreTypes.equals(this, (WildcardType) other);
        }

        @Override
        public int hashCode() {
            return MoreTypes.hashCode(this);
        }

        @Override
        public String toString() {
            return MoreTypes.toString(this);
        }
    }

    private static void checkNotPrimitive(final Type type, final String use) {
        if (type instanceof Class<?> && ((Class) type).isPrimitive()) {
            throw new IllegalArgumentException("Primitive types are not allowed in " + use + ": " + type);
        }
    }

    /**
     * A type formed from other types, such as arrays, parameterized types or wildcard types
     *
     * @opensearch.internal
     */
    private interface CompositeType {
        /**
         * Returns true if there are no type variables in this type.
         */
        boolean isFullySpecified();
    }
}
