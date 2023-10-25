/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2006 Google Inc.
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

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.inject.ConfigurationException;
import org.opensearch.common.inject.CreationException;
import org.opensearch.common.inject.Key;
import org.opensearch.common.inject.MembersInjector;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.inject.ProvisionException;
import org.opensearch.common.inject.Scope;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.common.inject.spi.Dependency;
import org.opensearch.common.inject.spi.InjectionListener;
import org.opensearch.common.inject.spi.InjectionPoint;
import org.opensearch.common.inject.spi.Message;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;

/**
 * A collection of error messages. If this type is passed as a method parameter, the method is
 * considered to have executed successfully only if new errors were not added to this collection.
 * <p>
 * Errors can be chained to provide additional context. To add context, call {@link #withSource}
 * to create a new Errors instance that contains additional context. All messages added to the
 * returned instance will contain full context.
 * <p>
 * To avoid messages with redundant context, {@link #withSource} should be added sparingly. A
 * good rule of thumb is to assume a method's caller has already specified enough context to
 * identify that method. When calling a method that's defined in a different context, call that
 * method with an errors object that includes its context.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 *
 * @opensearch.internal
 */
public final class Errors {

    /**
     * The root errors object. Used to access the list of error messages.
     */
    private final Errors root;

    /**
     * The parent errors object. Used to obtain the chain of source objects.
     */
    private final Errors parent;

    /**
     * The leaf source for errors added here.
     */
    private final Object source;

    /**
     * null unless (root == this) and error messages exist. Never an empty list.
     */
    private List<Message> errors; // lazy, use getErrorsForAdd()

    public Errors() {
        this.root = this;
        this.parent = null;
        this.source = SourceProvider.UNKNOWN_SOURCE;
    }

    public Errors(Object source) {
        this.root = this;
        this.parent = null;
        this.source = source;
    }

    private Errors(Errors parent, Object source) {
        this.root = parent.root;
        this.parent = parent;
        this.source = source;
    }

    /**
     * Returns an instance that uses {@code source} as a reference point for newly added errors.
     */
    public Errors withSource(final Object source) {
        return source == SourceProvider.UNKNOWN_SOURCE ? this : new Errors(this, source);
    }

    /**
     * We use a fairly generic error message here. The motivation is to share the
     * same message for both bind time errors:
     * <pre><code>Guice.createInjector(new AbstractModule() {
     *   public void configure() {
     *     bind(Runnable.class);
     *   }
     * }</code></pre>
     * ...and at provide-time errors:
     * <pre><code>Guice.createInjector().getInstance(Runnable.class);</code></pre>
     * Otherwise we need to know who's calling when resolving a just-in-time
     * binding, which makes things unnecessarily complex.
     */
    public Errors missingImplementation(final Key key) {
        return addMessage("No implementation for %s was bound.", key);
    }

    public Errors converterReturnedNull(
        final String stringValue,
        final Object source,
        final TypeLiteral<?> type,
        final MatcherAndConverter matchingConverter
    ) {
        return addMessage(
            "Received null converting '%s' (bound at %s) to %s%n" + " using %s.",
            stringValue,
            convert(source),
            type,
            matchingConverter
        );
    }

    public Errors conversionTypeError(
        final String stringValue,
        final Object source,
        final TypeLiteral<?> type,
        final MatcherAndConverter matchingConverter,
        final Object converted
    ) {
        return addMessage(
            "Type mismatch converting '%s' (bound at %s) to %s%n" + " using %s.%n" + " Converter returned %s.",
            stringValue,
            convert(source),
            type,
            matchingConverter,
            converted
        );
    }

    public Errors conversionError(
        final String stringValue,
        final Object source,
        final TypeLiteral<?> type,
        final MatcherAndConverter matchingConverter,
        final RuntimeException cause
    ) {
        return errorInUserCode(
            cause,
            "Error converting '%s' (bound at %s) to %s%n" + " using %s.%n" + " Reason: %s",
            stringValue,
            convert(source),
            type,
            matchingConverter,
            cause
        );
    }

    public Errors ambiguousTypeConversion(
        final String stringValue,
        final Object source,
        final TypeLiteral<?> type,
        final MatcherAndConverter a,
        final MatcherAndConverter b
    ) {
        return addMessage(
            "Multiple converters can convert '%s' (bound at %s) to %s:%n"
                + " %s and%n"
                + " %s.%n"
                + " Please adjust your type converter configuration to avoid overlapping matches.",
            stringValue,
            convert(source),
            type,
            a,
            b
        );
    }

    public Errors bindingToProvider() {
        return addMessage("Binding to Provider is not allowed.");
    }

    public Errors subtypeNotProvided(final Class<? extends Provider<?>> providerType, final Class<?> type) {
        return addMessage("%s doesn't provide instances of %s.", providerType, type);
    }

    public Errors notASubtype(final Class<?> implementationType, final Class<?> type) {
        return addMessage("%s doesn't extend %s.", implementationType, type);
    }

    public Errors recursiveImplementationType() {
        return addMessage("@ImplementedBy points to the same class it annotates.");
    }

    public Errors recursiveProviderType() {
        return addMessage("@ProvidedBy points to the same class it annotates.");
    }

    public Errors missingRuntimeRetention(final Object source) {
        return addMessage("Please annotate with @Retention(RUNTIME).%n" + " Bound at %s.", convert(source));
    }

    public Errors missingScopeAnnotation() {
        return addMessage("Please annotate with @ScopeAnnotation.");
    }

    public Errors optionalConstructor(final Constructor constructor) {
        return addMessage("%s is annotated @Inject(optional=true), " + "but constructors cannot be optional.", constructor);
    }

    public Errors cannotBindToGuiceType(final String simpleName) {
        return addMessage("Binding to core guice framework type is not allowed: %s.", simpleName);
    }

    public Errors scopeNotFound(final Class<? extends Annotation> scopeAnnotation) {
        return addMessage("No scope is bound to %s.", scopeAnnotation);
    }

    public Errors scopeAnnotationOnAbstractType(
        final Class<? extends Annotation> scopeAnnotation,
        final Class<?> type,
        final Object source
    ) {
        return addMessage(
            "%s is annotated with %s, but scope annotations are not supported " + "for abstract types.%n Bound at %s.",
            type,
            scopeAnnotation,
            convert(source)
        );
    }

    public Errors misplacedBindingAnnotation(final Member member, final Annotation bindingAnnotation) {
        return addMessage(
            "%s is annotated with %s, but binding annotations should be applied " + "to its parameters instead.",
            member,
            bindingAnnotation
        );
    }

    private static final String CONSTRUCTOR_RULES = "Classes must have either one (and only one) constructor "
        + "annotated with @Inject or a zero-argument constructor that is not private.";

    public Errors missingConstructor(final Class<?> implementation) {
        return addMessage("Could not find a suitable constructor in %s. " + CONSTRUCTOR_RULES, implementation);
    }

    public Errors tooManyConstructors(final Class<?> implementation) {
        return addMessage("%s has more than one constructor annotated with @Inject. " + CONSTRUCTOR_RULES, implementation);
    }

    public Errors duplicateScopes(final Scope existing, final Class<? extends Annotation> annotationType, final Scope scope) {
        return addMessage("Scope %s is already bound to %s. Cannot bind %s.", existing, annotationType, scope);
    }

    public Errors voidProviderMethod() {
        return addMessage("Provider methods must return a value. Do not return void.");
    }

    public Errors missingConstantValues() {
        return addMessage("Missing constant value. Please call to(...).");
    }

    public Errors cannotInjectInnerClass(final Class<?> type) {
        return addMessage(
            "Injecting into inner classes is not supported.  " + "Please use a 'static' class (top-level or nested) instead of %s.",
            type
        );
    }

    public Errors duplicateBindingAnnotations(
        final Member member,
        final Class<? extends Annotation> a,
        final Class<? extends Annotation> b
    ) {
        return addMessage("%s has more than one annotation annotated with @BindingAnnotation: " + "%s and %s", member, a, b);
    }

    public Errors duplicateScopeAnnotations(final Class<? extends Annotation> a, final Class<? extends Annotation> b) {
        return addMessage("More than one scope annotation was found: %s and %s.", a, b);
    }

    public Errors recursiveBinding() {
        return addMessage("Binding points to itself.");
    }

    public Errors bindingAlreadySet(final Key<?> key, final Object source) {
        return addMessage("A binding to %s was already configured at %s.", key, convert(source));
    }

    public Errors childBindingAlreadySet(final Key<?> key) {
        return addMessage("A binding to %s already exists on a child injector.", key);
    }

    public Errors errorInjectingMethod(final Throwable cause) {
        return errorInUserCode(cause, "Error injecting method, %s", cause);
    }

    public Errors errorInjectingConstructor(final Throwable cause) {
        return errorInUserCode(cause, "Error injecting constructor, %s", cause);
    }

    public Errors errorInProvider(final RuntimeException runtimeException) {
        return errorInUserCode(runtimeException, "Error in custom provider, %s", runtimeException);
    }

    public Errors errorInUserInjector(final MembersInjector<?> listener, final TypeLiteral<?> type, final RuntimeException cause) {
        return errorInUserCode(cause, "Error injecting %s using %s.%n" + " Reason: %s", type, listener, cause);
    }

    public Errors errorNotifyingInjectionListener(
        final InjectionListener<?> listener,
        final TypeLiteral<?> type,
        final RuntimeException cause
    ) {
        return errorInUserCode(cause, "Error notifying InjectionListener %s of %s.%n" + " Reason: %s", listener, type, cause);
    }

    public static Collection<Message> getMessagesFromThrowable(final Throwable throwable) {
        if (throwable instanceof ProvisionException) {
            return ((ProvisionException) throwable).getErrorMessages();
        } else if (throwable instanceof ConfigurationException) {
            return ((ConfigurationException) throwable).getErrorMessages();
        } else if (throwable instanceof CreationException) {
            return ((CreationException) throwable).getErrorMessages();
        } else {
            return emptySet();
        }
    }

    public Errors errorInUserCode(final Throwable cause, final String messageFormat, final Object... arguments) {
        Collection<Message> messages = getMessagesFromThrowable(cause);

        if (!messages.isEmpty()) {
            return merge(messages);
        } else {
            return addMessage(cause, messageFormat, arguments);
        }
    }

    public Errors cannotInjectRawProvider() {
        return addMessage("Cannot inject a Provider that has no type parameter");
    }

    public Errors cannotInjectRawMembersInjector() {
        return addMessage("Cannot inject a MembersInjector that has no type parameter");
    }

    public Errors cannotInjectTypeLiteralOf(final Type unsupportedType) {
        return addMessage("Cannot inject a TypeLiteral of %s", unsupportedType);
    }

    public Errors cannotInjectRawTypeLiteral() {
        return addMessage("Cannot inject a TypeLiteral that has no type parameter");
    }

    public Errors cannotSatisfyCircularDependency(final Class<?> expectedType) {
        return addMessage("Tried proxying %s to support a circular dependency, but it is not an interface.", expectedType);
    }

    public void throwCreationExceptionIfErrorsExist() {
        if (!hasErrors()) {
            return;
        }

        throw new CreationException(getMessages());
    }

    public void throwConfigurationExceptionIfErrorsExist() {
        if (!hasErrors()) {
            return;
        }

        throw new ConfigurationException(getMessages());
    }

    public void throwProvisionExceptionIfErrorsExist() {
        if (!hasErrors()) {
            return;
        }

        throw new ProvisionException(getMessages());
    }

    private Message merge(final Message message) {
        List<Object> sources = new ArrayList<>();
        sources.addAll(getSources());
        sources.addAll(message.getSources());
        return new Message(sources, message.getMessage(), message.getCause());
    }

    public Errors merge(final Collection<Message> messages) {
        for (Message message : messages) {
            addMessage(merge(message));
        }
        return this;
    }

    public Errors merge(final Errors moreErrors) {
        if (moreErrors.root == root || moreErrors.root.errors == null) {
            return this;
        }

        merge(moreErrors.root.errors);
        return this;
    }

    public List<Object> getSources() {
        List<Object> sources = new ArrayList<>();
        for (Errors e = this; e != null; e = e.parent) {
            if (e.source != SourceProvider.UNKNOWN_SOURCE) {
                sources.add(0, e.source);
            }
        }
        return sources;
    }

    public void throwIfNewErrors(final int expectedSize) throws ErrorsException {
        if (size() == expectedSize) {
            return;
        }

        throw toException();
    }

    public ErrorsException toException() {
        return new ErrorsException(this);
    }

    public boolean hasErrors() {
        return root.errors != null;
    }

    public Errors addMessage(final String messageFormat, final Object... arguments) {
        return addMessage(null, messageFormat, arguments);
    }

    private Errors addMessage(final Throwable cause, final String messageFormat, final Object... arguments) {
        final String message = format(messageFormat, arguments);
        addMessage(new Message(getSources(), message, cause));
        return this;
    }

    public Errors addMessage(final Message message) {
        if (root.errors == null) {
            root.errors = new ArrayList<>();
        }
        root.errors.add(message);
        return this;
    }

    public static String format(final String messageFormat, final Object... arguments) {
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = Errors.convert(arguments[i]);
        }
        return String.format(Locale.ROOT, messageFormat, arguments);
    }

    public List<Message> getMessages() {
        if (root.errors == null) {
            return Collections.emptyList();
        }

        final List<Message> result = new ArrayList<>(root.errors);
        CollectionUtil.timSort(result, Comparator.comparing(Message::getSource));

        return unmodifiableList(result);
    }

    /**
     * Returns the formatted message for an exception with the specified messages.
     */
    public static String format(final String heading, final Collection<Message> errorMessages) {
        try (Formatter fmt = new Formatter(Locale.ROOT)) {
            fmt.format(heading).format(":%n%n");
            int index = 1;
            final boolean displayCauses = getOnlyCause(errorMessages) == null;

            for (Message errorMessage : errorMessages) {
                fmt.format("%s) %s%n", index++, errorMessage.getMessage());

                final List<Object> dependencies = errorMessage.getSources();
                for (int i = dependencies.size() - 1; i >= 0; i--) {
                    final Object source = dependencies.get(i);
                    formatSource(fmt, source);
                }

                final Throwable cause = errorMessage.getCause();
                if (displayCauses && cause != null) {
                    StringWriter writer = new StringWriter();
                    cause.printStackTrace(new PrintWriter(writer));
                    fmt.format("Caused by: %s", writer.getBuffer());
                }

                fmt.format("%n");
            }

            if (errorMessages.size() == 1) {
                fmt.format("1 error");
            } else {
                fmt.format("%s errors", errorMessages.size());
            }

            return fmt.toString();
        }
    }

    /**
     * Returns {@code value} if it is non-null allowed to be null. Otherwise a message is added and
     * an {@code ErrorsException} is thrown.
     */
    public <T> T checkForNull(final T value, final Object source, final Dependency<?> dependency) throws ErrorsException {
        if (value != null || dependency.isNullable()) {
            return value;
        }

        final int parameterIndex = dependency.getParameterIndex();
        final String parameterName = (parameterIndex != -1) ? "parameter " + parameterIndex + " of " : "";
        addMessage(
            "null returned by binding at %s%n but %s%s is not @Nullable",
            source,
            parameterName,
            dependency.getInjectionPoint().getMember()
        );

        throw toException();
    }

    /**
     * Returns the cause throwable if there is exactly one cause in {@code messages}. If there are
     * zero or multiple messages with causes, null is returned.
     */
    public static Throwable getOnlyCause(final Collection<Message> messages) {
        Throwable onlyCause = null;
        for (Message message : messages) {
            Throwable messageCause = message.getCause();
            if (messageCause == null) {
                continue;
            }

            if (onlyCause != null) {
                return null;
            }

            onlyCause = messageCause;
        }

        return onlyCause;
    }

    public int size() {
        return root.errors == null ? 0 : root.errors.size();
    }

    /**
     * A converter
     *
     * @opensearch.internal
     */
    private abstract static class Converter<T> {

        final Class<T> type;

        Converter(final Class<T> type) {
            this.type = type;
        }

        boolean appliesTo(final Object o) {
            return type.isAssignableFrom(o.getClass());
        }

        String convert(final Object o) {
            return toString(type.cast(o));
        }

        abstract String toString(final T t);
    }

    private static final Collection<Converter<?>> converters = Arrays.asList(new Converter<>(Class.class) {
        @Override
        public String toString(final Class c) {
            return c.getName();
        }
    }, new Converter<>(Member.class) {
        @Override
        public String toString(final Member member) {
            return MoreTypes.toString(member);
        }
    }, new Converter<>(Key.class) {
        @Override
        public String toString(final Key key) {
            if (key.getAnnotationType() != null) {
                return key.getTypeLiteral()
                    + " annotated with "
                    + (key.getAnnotation() != null ? key.getAnnotation() : key.getAnnotationType());
            } else {
                return key.getTypeLiteral().toString();
            }
        }
    });

    public static Object convert(final Object o) {
        for (Converter<?> converter : converters) {
            if (converter.appliesTo(o)) {
                return converter.convert(o);
            }
        }
        return o;
    }

    public static void formatSource(final Formatter formatter, final Object source) {
        if (source instanceof Dependency) {
            final Dependency<?> dependency = (Dependency<?>) source;
            final InjectionPoint injectionPoint = dependency.getInjectionPoint();
            if (injectionPoint != null) {
                formatInjectionPoint(formatter, dependency, injectionPoint);
            } else {
                formatSource(formatter, dependency.getKey());
            }

        } else if (source instanceof InjectionPoint) {
            formatInjectionPoint(formatter, null, (InjectionPoint) source);

        } else if (source instanceof Class) {
            formatter.format("  at %s%n", StackTraceElements.forType((Class<?>) source));

        } else if (source instanceof Member) {
            formatter.format("  at %s%n", StackTraceElements.forMember((Member) source));

        } else if (source instanceof TypeLiteral) {
            formatter.format("  while locating %s%n", source);

        } else if (source instanceof Key) {
            Key<?> key = (Key<?>) source;
            formatter.format("  while locating %s%n", convert(key));

        } else {
            formatter.format("  at %s%n", source);
        }
    }

    public static void formatInjectionPoint(final Formatter formatter, Dependency<?> dependency, final InjectionPoint injectionPoint) {
        final Member member = injectionPoint.getMember();
        final Class<? extends Member> memberType = MoreTypes.memberType(member);

        if (memberType == Field.class) {
            dependency = injectionPoint.getDependencies().get(0);
            formatter.format("  while locating %s%n", convert(dependency.getKey()));
            formatter.format("    for field at %s%n", StackTraceElements.forMember(member));

        } else if (dependency != null) {
            formatter.format("  while locating %s%n", convert(dependency.getKey()));
            formatter.format("    for parameter %s at %s%n", dependency.getParameterIndex(), StackTraceElements.forMember(member));

        } else {
            formatSource(formatter, injectionPoint.getMember());
        }
    }
}
