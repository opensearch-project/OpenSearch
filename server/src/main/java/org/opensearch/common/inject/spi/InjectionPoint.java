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

package org.opensearch.common.inject.spi;

import org.opensearch.common.inject.ConfigurationException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.internal.Annotations;
import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.internal.ErrorsException;
import org.opensearch.common.inject.internal.MoreTypes;
import org.opensearch.common.inject.internal.Nullability;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.opensearch.common.inject.internal.MoreTypes.getRawType;

/**
 * A constructor, field or method that can receive injections. Typically this is a member with the
 * {@literal @}{@link Inject} annotation. For non-private, no argument constructors, the member may
 * omit the annotation.
 *
 * @author crazybob@google.com (Bob Lee)
 * @since 2.0
 *
 * @opensearch.internal
 */
public final class InjectionPoint {

    private final boolean optional;
    private final Member member;
    private final List<Dependency<?>> dependencies;

    InjectionPoint(final TypeLiteral<?> type, final Method method) {
        this.member = method;

        Inject inject = method.getAnnotation(Inject.class);
        this.optional = inject.optional();

        this.dependencies = forMember(method, type, method.getParameterAnnotations());
    }

    InjectionPoint(final TypeLiteral<?> type, final Constructor<?> constructor) {
        this.member = constructor;
        this.optional = false;
        this.dependencies = forMember(constructor, type, constructor.getParameterAnnotations());
    }

    InjectionPoint(final TypeLiteral<?> type, final Field field) {
        this.member = field;

        final Inject inject = field.getAnnotation(Inject.class);
        this.optional = inject.optional();

        final Annotation[] annotations = field.getAnnotations();

        final Errors errors = new Errors(field);
        Key<?> key = null;
        try {
            key = Annotations.getKey(type.getFieldType(field), field, annotations, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }
        errors.throwConfigurationExceptionIfErrorsExist();

        this.dependencies = Collections.singletonList(newDependency(key, Nullability.allowsNull(annotations), -1));
    }

    private List<Dependency<?>> forMember(final Member member, final TypeLiteral<?> type, final Annotation[][] parameterAnnotations) {
        final Errors errors = new Errors(member);
        final Iterator<Annotation[]> annotationsIterator = Arrays.asList(parameterAnnotations).iterator();

        final List<Dependency<?>> dependencies = new ArrayList<>();
        int index = 0;

        for (TypeLiteral<?> parameterType : type.getParameterTypes(member)) {
            try {
                final Annotation[] paramAnnotations = annotationsIterator.next();
                final Key<?> key = Annotations.getKey(parameterType, member, paramAnnotations, errors);
                dependencies.add(newDependency(key, Nullability.allowsNull(paramAnnotations), index));
                index++;
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        errors.throwConfigurationExceptionIfErrorsExist();
        return Collections.unmodifiableList(dependencies);
    }

    // This method is necessary to create a Dependency<T> with proper generic type information
    private <T> Dependency<T> newDependency(final Key<T> key, final boolean allowsNull, final int parameterIndex) {
        return new Dependency<>(this, key, allowsNull, parameterIndex);
    }

    /**
     * Returns the injected constructor, field, or method.
     */
    public Member getMember() {
        return member;
    }

    /**
     * Returns the dependencies for this injection point. If the injection point is for a method or
     * constructor, the dependencies will correspond to that member's parameters. Field injection
     * points always have a single dependency for the field itself.
     *
     * @return a possibly-empty list
     */
    public List<Dependency<?>> getDependencies() {
        return dependencies;
    }

    /**
     * Returns true if this injection point shall be skipped if the injector cannot resolve bindings
     * for all required dependencies. Both explicit bindings (as specified in a module), and implicit
     * bindings ({@literal @}{@link org.opensearch.common.inject.ImplementedBy ImplementedBy}, default
     * constructors etc.) may be used to satisfy optional injection points.
     */
    public boolean isOptional() {
        return optional;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof InjectionPoint && member.equals(((InjectionPoint) o).member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return MoreTypes.toString(member);
    }

    /**
     * Returns a new injection point for the injectable constructor of {@code type}.
     *
     * @param type a concrete type with exactly one constructor annotated {@literal @}{@link Inject},
     *             or a no-arguments constructor that is not private.
     * @throws ConfigurationException if there is no injectable constructor, more than one injectable
     *                                constructor, or if parameters of the injectable constructor are malformed, such as a
     *                                parameter with multiple binding annotations.
     */
    public static InjectionPoint forConstructorOf(final TypeLiteral<?> type) {
        final Class<?> rawType = getRawType(type.getType());
        final Errors errors = new Errors(rawType);

        Constructor<?> injectableConstructor = null;
        for (Constructor<?> constructor : rawType.getConstructors()) {
            final Inject inject = constructor.getAnnotation(Inject.class);
            if (inject != null) {
                if (inject.optional()) {
                    errors.optionalConstructor(constructor);
                }

                if (injectableConstructor != null) {
                    errors.tooManyConstructors(rawType);
                }

                injectableConstructor = constructor;
                checkForMisplacedBindingAnnotations(injectableConstructor, errors);
            }
        }

        errors.throwConfigurationExceptionIfErrorsExist();

        if (injectableConstructor != null) {
            return new InjectionPoint(type, injectableConstructor);
        }

        // If no annotated constructor is found, look for a no-arg constructor instead.
        try {
            final Constructor<?> noArgConstructor = rawType.getConstructor();

            // Disallow private constructors on non-private classes (unless they have @Inject)
            if (Modifier.isPrivate(noArgConstructor.getModifiers()) && !Modifier.isPrivate(rawType.getModifiers())) {
                errors.missingConstructor(rawType);
                throw new ConfigurationException(errors.getMessages());
            }

            checkForMisplacedBindingAnnotations(noArgConstructor, errors);
            return new InjectionPoint(type, noArgConstructor);
        } catch (NoSuchMethodException e) {
            errors.missingConstructor(rawType);
            throw new ConfigurationException(errors.getMessages());
        }
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(final TypeLiteral<?> type) {
        Set<InjectionPoint> result = new HashSet<>();
        final Errors errors = new Errors();

        // TODO (crazybob): Filter out overridden members.
        addInjectionPoints(type, Factory.FIELDS, false, result, errors);
        addInjectionPoints(type, Factory.METHODS, false, result, errors);

        result = unmodifiableSet(result);
        if (errors.hasErrors()) {
            throw new ConfigurationException(errors.getMessages()).withPartialValue(result);
        }
        return result;
    }

    /**
     * Returns all instance method and field injection points on {@code type}.
     *
     * @return a possibly empty set of injection points. The set has a specified iteration order. All
     *         fields are returned and then all methods. Within the fields, supertype fields are returned
     *         before subtype fields. Similarly, supertype methods are returned before subtype methods.
     * @throws ConfigurationException if there is a malformed injection point on {@code type}, such as
     *                                a field with multiple binding annotations. The exception's {@link
     *                                ConfigurationException#getPartialValue() partial value} is a {@code Set<InjectionPoint>}
     *                                of the valid injection points.
     */
    public static Set<InjectionPoint> forInstanceMethodsAndFields(final Class<?> type) {
        return forInstanceMethodsAndFields(TypeLiteral.get(type));
    }

    private static void checkForMisplacedBindingAnnotations(final Member member, final Errors errors) {
        Annotation misplacedBindingAnnotation = Annotations.findBindingAnnotation(
            errors,
            member,
            ((AnnotatedElement) member).getAnnotations()
        );
        if (misplacedBindingAnnotation == null) {
            return;
        }

        // don't warn about misplaced binding annotations on methods when there's a field with the same
        // name. In Scala, fields always get accessor methods (that we need to ignore). See bug 242.
        if (member instanceof Method) {
            try {
                if (member.getDeclaringClass().getField(member.getName()) != null) {
                    return;
                }
            } catch (NoSuchFieldException ignore) {}
        }

        errors.misplacedBindingAnnotation(member, misplacedBindingAnnotation);
    }

    private static <M extends Member & AnnotatedElement> void addInjectionPoints(
        final TypeLiteral<?> type,
        final Factory<M> factory,
        final boolean statics,
        final Collection<InjectionPoint> injectionPoints,
        final Errors errors
    ) {
        if (type.getType() == Object.class) {
            return;
        }

        // Add injectors for superclass first.
        final TypeLiteral<?> superType = type.getSupertype(type.getRawType().getSuperclass());
        addInjectionPoints(superType, factory, statics, injectionPoints, errors);

        // Add injectors for all members next
        addInjectorsForMembers(type, factory, statics, injectionPoints, errors);
    }

    private static <M extends Member & AnnotatedElement> void addInjectorsForMembers(
        final TypeLiteral<?> typeLiteral,
        final Factory<M> factory,
        final boolean statics,
        final Collection<InjectionPoint> injectionPoints,
        final Errors errors
    ) {
        for (M member : factory.getMembers(getRawType(typeLiteral.getType()))) {
            if (isStatic(member) != statics) {
                continue;
            }

            final Inject inject = member.getAnnotation(Inject.class);
            if (inject == null) {
                continue;
            }

            try {
                injectionPoints.add(factory.create(typeLiteral, member, errors));
            } catch (ConfigurationException ignorable) {
                if (!inject.optional()) {
                    errors.merge(ignorable.getErrorMessages());
                }
            }
        }
    }

    private static boolean isStatic(Member member) {
        return Modifier.isStatic(member.getModifiers());
    }

    /**
     * Factory for the injection point
     *
     * @opensearch.internal
     */
    private interface Factory<M extends Member & AnnotatedElement> {
        Factory<Field> FIELDS = new Factory<>() {
            @Override
            public Field[] getMembers(Class<?> type) {
                return type.getFields();
            }

            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Field member, Errors errors) {
                return new InjectionPoint(typeLiteral, member);
            }
        };

        Factory<Method> METHODS = new Factory<>() {
            @Override
            public Method[] getMembers(Class<?> type) {
                return type.getMethods();
            }

            @Override
            public InjectionPoint create(TypeLiteral<?> typeLiteral, Method member, Errors errors) {
                checkForMisplacedBindingAnnotations(member, errors);
                return new InjectionPoint(typeLiteral, member);
            }
        };

        M[] getMembers(Class<?> type);

        InjectionPoint create(TypeLiteral<?> typeLiteral, M member, Errors errors);
    }
}
