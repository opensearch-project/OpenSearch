/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.DeprecatedApi;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.annotation.PublicApi;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.AnnotatedConstruct;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.ReferenceType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;
import javax.lang.model.type.WildcardType;
import javax.tools.Diagnostic.Kind;

import java.util.HashSet;
import java.util.Set;

/**
 * The annotation processor for API related annotations: {@link DeprecatedApi}, {@link ExperimentalApi},
 * {@link InternalApi} and {@link PublicApi}.
 * <p>
 * The checks are built on top of the following rules:
 * <ul>
 *  <li>introspect each type annotated with {@link PublicApi}, {@link DeprecatedApi} or {@link ExperimentalApi},
 *  filtering out package-private declarations</li>
 *  <li>make sure those leak only {@link PublicApi}, {@link DeprecatedApi} or {@link ExperimentalApi} types as well (exceptions,
 *    method return values, method arguments, method generic type arguments, class generic type arguments,  annotations)</li>
 *  <li>recursively follow the type introspection chains to enforce the rules down the line</li>
 * </ul>
 */
@InternalApi
@SupportedAnnotationTypes("org.opensearch.common.annotation.*")
public class ApiAnnotationProcessor extends AbstractProcessor {
    private static final String OPTION_CONTINUE_ON_FAILING_CHECKS = "continueOnFailingChecks";
    private static final String OPENSEARCH_PACKAGE = "org.opensearch";

    private final Set<Element> reported = new HashSet<>();
    private final Set<AnnotatedConstruct> processed = new HashSet<>();
    private Kind reportFailureAs = Kind.ERROR;

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    @Override
    public Set<String> getSupportedOptions() {
        return Set.of(OPTION_CONTINUE_ON_FAILING_CHECKS);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment round) {
        processingEnv.getMessager().printMessage(Kind.NOTE, "Processing OpenSearch Api annotations");

        if (processingEnv.getOptions().containsKey(OPTION_CONTINUE_ON_FAILING_CHECKS) == true) {
            reportFailureAs = Kind.NOTE;
        }

        final Set<? extends Element> elements = round.getElementsAnnotatedWithAny(
            Set.of(PublicApi.class, ExperimentalApi.class, DeprecatedApi.class)
        );

        for (var element : elements) {
            if (!checkPackage(element)) {
                continue;
            }

            // Skip all not-public elements
            checkPublicVisibility(null, element);

            if (element instanceof TypeElement) {
                process((TypeElement) element);
            }
        }

        return false;
    }

    /**
     * Check top level executable element
     * @param executable top level executable element
     * @param enclosing enclosing element
     */
    private void process(ExecutableElement executable, Element enclosing) {
        if (!inspectable(executable)) {
            return;
        }

        // The executable element should not be internal (unless constructor for injectable core component)
        checkNotInternal(enclosing, executable);

        // Check this element's annotations
        for (final AnnotationMirror annotation : executable.getAnnotationMirrors()) {
            final Element element = annotation.getAnnotationType().asElement();
            if (inspectable(element)) {
                checkNotInternal(executable.getEnclosingElement(), element);
                checkPublic(executable.getEnclosingElement(), element);
            }
        }

        // Process method return types
        final TypeMirror returnType = executable.getReturnType();
        if (returnType instanceof ReferenceType) {
            process(executable, (ReferenceType) returnType);
        }

        // Process method thrown types
        for (final TypeMirror thrownType : executable.getThrownTypes()) {
            if (thrownType instanceof ReferenceType) {
                process(executable, (ReferenceType) thrownType);
            }
        }

        // Process method type parameters
        for (final TypeParameterElement typeParameter : executable.getTypeParameters()) {
            for (final TypeMirror boundType : typeParameter.getBounds()) {
                if (boundType instanceof ReferenceType) {
                    process(executable, (ReferenceType) boundType);
                }
            }
        }

        // Process method arguments
        for (final VariableElement parameter : executable.getParameters()) {
            final TypeMirror parameterType = parameter.asType();
            if (parameterType instanceof ReferenceType) {
                process(executable, (ReferenceType) parameterType);
            }
        }
    }

    /**
     * Check wildcard type bounds referred by an element
     * @param executable element
     * @param type wildcard type
     */
    private void process(ExecutableElement executable, WildcardType type) {
        if (type.getExtendsBound() instanceof ReferenceType) {
            process(executable, (ReferenceType) type.getExtendsBound());
        }

        if (type.getSuperBound() instanceof ReferenceType) {
            process(executable, (ReferenceType) type.getSuperBound());
        }
    }

    /**
     * Check reference type bounds referred by an executable element
     * @param executable executable element
     * @param ref reference type
     */
    private void process(ExecutableElement executable, ReferenceType ref) {
        // The element has been processed already
        if (processed.add(ref) == false) {
            return;
        }

        if (ref instanceof DeclaredType) {
            final DeclaredType declaredType = (DeclaredType) ref;

            final Element element = declaredType.asElement();
            if (inspectable(element)) {
                checkNotInternal(executable.getEnclosingElement(), element);
                checkPublic(executable.getEnclosingElement(), element);
            }

            for (final TypeMirror type : declaredType.getTypeArguments()) {
                if (type instanceof ReferenceType) {
                    process(executable, (ReferenceType) type);
                } else if (type instanceof WildcardType) {
                    process(executable, (WildcardType) type);
                }
            }
        } else if (ref instanceof ArrayType) {
            final TypeMirror componentType = ((ArrayType) ref).getComponentType();
            if (componentType instanceof ReferenceType) {
                process(executable, (ReferenceType) componentType);
            }
        } else if (ref instanceof TypeVariable) {
            final TypeVariable typeVariable = (TypeVariable) ref;
            if (typeVariable.getUpperBound() instanceof ReferenceType) {
                process(executable, (ReferenceType) typeVariable.getUpperBound());
            }
            if (typeVariable.getLowerBound() instanceof ReferenceType) {
                process(executable, (ReferenceType) typeVariable.getLowerBound());
            }
        }

        // Check this element's annotations
        for (final AnnotationMirror annotation : ref.getAnnotationMirrors()) {
            final Element element = annotation.getAnnotationType().asElement();
            if (inspectable(element)) {
                checkNotInternal(executable.getEnclosingElement(), element);
                checkPublic(executable.getEnclosingElement(), element);
            }
        }
    }

    /**
     * Check if a particular executable element should be inspected or not
     * @param executable executable element to inspect
     * @return {@code true} if a particular executable element should be inspected, {@code false} otherwise
     */
    private boolean inspectable(ExecutableElement executable) {
        // The constructors for public APIs could use non-public APIs when those are supposed to be only
        // consumed (not instantiated) by external consumers.
        return executable.getKind() != ElementKind.CONSTRUCTOR && executable.getModifiers().contains(Modifier.PUBLIC);
    }

    /**
     * Check if a particular element should be inspected or not
     * @param element element to inspect
     * @return {@code true} if a particular element should be inspected, {@code false} otherwise
     */
    private boolean inspectable(Element element) {
        final PackageElement pckg = processingEnv.getElementUtils().getPackageOf(element);
        return pckg.getQualifiedName().toString().startsWith(OPENSEARCH_PACKAGE)
            && !element.getEnclosingElement()
                .getAnnotationMirrors()
                .stream()
                .anyMatch(
                    m -> m.getAnnotationType()
                        .toString() /* ClassSymbol.toString() returns class name */
                        .equalsIgnoreCase("javax.annotation.Generated")
                );
    }

    /**
     * Check if a particular element belongs to OpenSeach managed packages
     * @param element element to inspect
     * @return {@code true} if a particular element belongs to OpenSeach managed packages, {@code false} otherwise
     */
    private boolean checkPackage(Element element) {
        // The element was reported already
        if (reported.contains(element)) {
            return false;
        }

        final PackageElement pckg = processingEnv.getElementUtils().getPackageOf(element);
        final boolean belongsToOpenSearch = pckg.getQualifiedName().toString().startsWith(OPENSEARCH_PACKAGE);

        if (!belongsToOpenSearch) {
            reported.add(element);

            processingEnv.getMessager()
                .printMessage(
                    reportFailureAs,
                    "The type "
                        + element
                        + " is not residing in "
                        + OPENSEARCH_PACKAGE
                        + ".* package "
                        + "and should not be annotated as OpenSearch APIs."
                );
        }

        return belongsToOpenSearch;
    }

    /**
     * Check the fields, methods, constructors, and member types that are directly
     * declared in this class or interface.
     * @param element class or interface
     */
    private void process(Element element) {
        // Check the fields, methods, constructors, and member types that are directly
        // declared in this class or interface.
        for (final Element enclosed : element.getEnclosedElements()) {
            // Skip all not-public elements
            if (!enclosed.getModifiers().contains(Modifier.PUBLIC)) {
                continue;
            }

            if (enclosed instanceof ExecutableElement) {
                process((ExecutableElement) enclosed, element);
            }
        }
    }

    /**
     * Check if element is public and annotated with {@link PublicApi}, {@link DeprecatedApi} or {@link ExperimentalApi}
     * @param referencedBy the referrer for the element
     * @param element element to check
     */
    private void checkPublic(@Nullable Element referencedBy, final Element element) {
        // The element was reported already
        if (reported.contains(element)) {
            return;
        }

        checkPublicVisibility(referencedBy, element);

        if (element.getAnnotation(PublicApi.class) == null
            && element.getAnnotation(ExperimentalApi.class) == null
            && element.getAnnotation(DeprecatedApi.class) == null) {
            reported.add(element);

            processingEnv.getMessager()
                .printMessage(
                    reportFailureAs,
                    "The element "
                        + element
                        + " is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi"
                        + ((referencedBy != null) ? " (referenced by " + referencedBy + ") " : "")
                );
        }
    }

    /**
     * Check if element has public visibility (following Java visibility rules)
     * @param referencedBy the referrer for the element
     * @param element element to check
     */
    private void checkPublicVisibility(Element referencedBy, final Element element) {
        if (!element.getModifiers().contains(Modifier.PUBLIC) && !element.getModifiers().contains(Modifier.PROTECTED)) {
            reported.add(element);

            processingEnv.getMessager()
                .printMessage(
                    reportFailureAs,
                    "The element "
                        + element
                        + " is part of the public APIs but does not have public or protected visibility"
                        + ((referencedBy != null) ? " (referenced by " + referencedBy + ") " : "")
                );
        }
    }

    /**
     * Check if element is not annotated with {@link InternalApi}
     * @param referencedBy the referrer for the element
     * @param element element to check
     */
    private void checkNotInternal(@Nullable Element referencedBy, final Element element) {
        // The element was reported already
        if (reported.contains(element)) {
            return;
        }

        if (element.getAnnotation(InternalApi.class) != null) {
            reported.add(element);

            processingEnv.getMessager()
                .printMessage(
                    reportFailureAs,
                    "The element "
                        + element
                        + " is part of the public APIs but is marked as @InternalApi"
                        + ((referencedBy != null) ? " (referenced by " + referencedBy + ") " : "")
                );
        }
    }
}
