/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.painless.lookup;

import org.opensearch.bootstrap.BootstrapInfo;
import org.opensearch.painless.Def;
import org.opensearch.painless.MethodWriter;
import org.opensearch.painless.WriterConstants;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistClass;
import org.opensearch.painless.spi.AllowlistClassBinding;
import org.opensearch.painless.spi.AllowlistConstructor;
import org.opensearch.painless.spi.AllowlistField;
import org.opensearch.painless.spi.AllowlistInstanceBinding;
import org.opensearch.painless.spi.AllowlistMethod;
import org.opensearch.painless.spi.annotation.InjectConstantAnnotation;
import org.opensearch.painless.spi.annotation.NoImportAnnotation;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.PrivilegedAction;
import java.security.SecureClassLoader;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.opensearch.painless.WriterConstants.DEF_TO_B_BYTE_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_CHARACTER_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_DOUBLE_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_FLOAT_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_INTEGER_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_LONG_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_TO_B_SHORT_IMPLICIT;
import static org.opensearch.painless.WriterConstants.DEF_UTIL_TYPE;
import static org.opensearch.painless.WriterConstants.OBJECT_TYPE;
import static org.opensearch.painless.lookup.PainlessLookupUtility.DEF_CLASS_NAME;
import static org.opensearch.painless.lookup.PainlessLookupUtility.buildPainlessConstructorKey;
import static org.opensearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.opensearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.opensearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;
import static org.opensearch.painless.lookup.PainlessLookupUtility.typeToJavaType;
import static org.opensearch.painless.lookup.PainlessLookupUtility.typesToCanonicalTypeNames;

public final class PainlessLookupBuilder {

    private static final class BridgeLoader extends SecureClassLoader {
        BridgeLoader(ClassLoader parent) {
            super(parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
            return Def.class.getName().equals(name) ? Def.class : super.findClass(name);
        }

        Class<?> defineBridge(String name, byte[] bytes) {
            return defineClass(name, bytes, 0, bytes.length, CODESOURCE);
        }
    }

    private static final CodeSource CODESOURCE;

    private static final Map<PainlessConstructor, PainlessConstructor> painlessConstructorCache = new HashMap<>();
    private static final Map<PainlessMethod, PainlessMethod> painlessMethodCache = new HashMap<>();
    private static final Map<PainlessField, PainlessField> painlessFieldCache = new HashMap<>();
    private static final Map<PainlessClassBinding, PainlessClassBinding> painlessClassBindingCache = new HashMap<>();
    private static final Map<PainlessInstanceBinding, PainlessInstanceBinding> painlessInstanceBindingCache = new HashMap<>();
    private static final Map<PainlessMethod, PainlessMethod> painlessBridgeCache = new HashMap<>();

    private static final Pattern CLASS_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][._a-zA-Z0-9]*$");
    private static final Pattern METHOD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");

    static {
        try {
            CODESOURCE = new CodeSource(URI.create("file:" + BootstrapInfo.UNTRUSTED_CODEBASE).toURL(), (Certificate[]) null);
        } catch (MalformedURLException mue) {
            throw new RuntimeException(mue);
        }
    }

    public static PainlessLookup buildFromAllowlists(List<Allowlist> allowlists) {
        PainlessLookupBuilder painlessLookupBuilder = new PainlessLookupBuilder();
        String origin = "internal error";

        try {
            for (Allowlist allowlist : allowlists) {
                for (AllowlistClass allowlistClass : allowlist.allowlistClasses) {
                    origin = allowlistClass.origin;
                    painlessLookupBuilder.addPainlessClass(
                        allowlist.classLoader,
                        allowlistClass.javaClassName,
                        allowlistClass.painlessAnnotations.containsKey(NoImportAnnotation.class) == false
                    );
                }
            }

            for (Allowlist allowlist : allowlists) {
                for (AllowlistClass allowlistClass : allowlist.allowlistClasses) {
                    String targetCanonicalClassName = allowlistClass.javaClassName.replace('$', '.');

                    for (AllowlistConstructor allowlistConstructor : allowlistClass.allowlistConstructors) {
                        origin = allowlistConstructor.origin;
                        painlessLookupBuilder.addPainlessConstructor(
                            targetCanonicalClassName,
                            allowlistConstructor.canonicalTypeNameParameters,
                            allowlistConstructor.painlessAnnotations
                        );
                    }

                    for (AllowlistMethod allowlistMethod : allowlistClass.allowlistMethods) {
                        origin = allowlistMethod.origin;
                        painlessLookupBuilder.addPainlessMethod(
                            allowlist.classLoader,
                            targetCanonicalClassName,
                            allowlistMethod.augmentedCanonicalClassName,
                            allowlistMethod.methodName,
                            allowlistMethod.returnCanonicalTypeName,
                            allowlistMethod.canonicalTypeNameParameters,
                            allowlistMethod.painlessAnnotations
                        );
                    }

                    for (AllowlistField allowlistField : allowlistClass.allowlistFields) {
                        origin = allowlistField.origin;
                        painlessLookupBuilder.addPainlessField(
                            targetCanonicalClassName,
                            allowlistField.fieldName,
                            allowlistField.canonicalTypeNameParameter
                        );
                    }
                }

                for (AllowlistMethod allowlistStatic : allowlist.allowlistImportedMethods) {
                    origin = allowlistStatic.origin;
                    painlessLookupBuilder.addImportedPainlessMethod(
                        allowlist.classLoader,
                        allowlistStatic.augmentedCanonicalClassName,
                        allowlistStatic.methodName,
                        allowlistStatic.returnCanonicalTypeName,
                        allowlistStatic.canonicalTypeNameParameters,
                        allowlistStatic.painlessAnnotations
                    );
                }

                for (AllowlistClassBinding allowlistClassBinding : allowlist.allowlistClassBindings) {
                    origin = allowlistClassBinding.origin;
                    painlessLookupBuilder.addPainlessClassBinding(
                        allowlist.classLoader,
                        allowlistClassBinding.targetJavaClassName,
                        allowlistClassBinding.methodName,
                        allowlistClassBinding.returnCanonicalTypeName,
                        allowlistClassBinding.canonicalTypeNameParameters,
                        allowlistClassBinding.painlessAnnotations
                    );
                }

                for (AllowlistInstanceBinding allowlistInstanceBinding : allowlist.allowlistInstanceBindings) {
                    origin = allowlistInstanceBinding.origin;
                    painlessLookupBuilder.addPainlessInstanceBinding(
                        allowlistInstanceBinding.targetInstance,
                        allowlistInstanceBinding.methodName,
                        allowlistInstanceBinding.returnCanonicalTypeName,
                        allowlistInstanceBinding.canonicalTypeNameParameters
                    );
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading allowlist(s) " + origin, exception);
        }

        return painlessLookupBuilder.build();
    }

    // javaClassNamesToClasses is all the classes that need to be available to the custom classloader
    // including classes used as part of imported methods and class bindings but not necessarily allowlisted
    // individually. The values of javaClassNamesToClasses are a superset of the values of
    // canonicalClassNamesToClasses.
    private final Map<String, Class<?>> javaClassNamesToClasses;
    // canonicalClassNamesToClasses is all the allowlisted classes available in a Painless script including
    // classes with imported canonical names but does not include classes from imported methods or class
    // bindings unless also allowlisted separately. The values of canonicalClassNamesToClasses are a subset
    // of the values of javaClassNamesToClasses.
    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClassBuilder> classesToPainlessClassBuilders;

    private final Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods;
    private final Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings;
    private final Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings;

    public PainlessLookupBuilder() {
        javaClassNamesToClasses = new HashMap<>();
        canonicalClassNamesToClasses = new HashMap<>();
        classesToPainlessClassBuilders = new HashMap<>();

        painlessMethodKeysToImportedPainlessMethods = new HashMap<>();
        painlessMethodKeysToPainlessClassBindings = new HashMap<>();
        painlessMethodKeysToPainlessInstanceBindings = new HashMap<>();
    }

    private Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    private boolean isValidType(Class<?> type) {
        while (type.getComponentType() != null) {
            type = type.getComponentType();
        }

        return type == def.class || classesToPainlessClassBuilders.containsKey(type);
    }

    public void addPainlessClass(ClassLoader classLoader, String javaClassName, boolean importClassName) {
        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);

        Class<?> clazz;

        if ("void".equals(javaClassName)) clazz = void.class;
        else if ("boolean".equals(javaClassName)) clazz = boolean.class;
        else if ("byte".equals(javaClassName)) clazz = byte.class;
        else if ("short".equals(javaClassName)) clazz = short.class;
        else if ("char".equals(javaClassName)) clazz = char.class;
        else if ("int".equals(javaClassName)) clazz = int.class;
        else if ("long".equals(javaClassName)) clazz = long.class;
        else if ("float".equals(javaClassName)) clazz = float.class;
        else if ("double".equals(javaClassName)) clazz = double.class;
        else {
            try {
                clazz = Class.forName(javaClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("class [" + javaClassName + "] not found", cnfe);
            }
        }

        addPainlessClass(clazz, importClassName);
    }

    public void addPainlessClass(Class<?> clazz, boolean importClassName) {
        Objects.requireNonNull(clazz);
        // Matcher m = new Matcher();

        if (clazz == def.class) {
            throw new IllegalArgumentException("cannot add reserved class [" + DEF_CLASS_NAME + "]");
        }

        String canonicalClassName = typeToCanonicalTypeName(clazz);

        if (clazz.isArray()) {
            throw new IllegalArgumentException("cannot add array type [" + canonicalClassName + "] as a class");
        }

        if (CLASS_NAME_PATTERN.matcher(canonicalClassName).matches() == false) {
            throw new IllegalArgumentException("invalid class name [" + canonicalClassName + "]");
        }

        Class<?> existingClass = javaClassNamesToClasses.get(clazz.getName());

        if (existingClass == null) {
            javaClassNamesToClasses.put(clazz.getName().intern(), clazz);
        } else if (existingClass != clazz) {
            throw new IllegalArgumentException(
                "class ["
                    + canonicalClassName
                    + "] "
                    + "cannot represent multiple java classes with the same name from different class loaders"
            );
        }

        existingClass = canonicalClassNamesToClasses.get(canonicalClassName);

        if (existingClass != null && existingClass != clazz) {
            throw new IllegalArgumentException(
                "class ["
                    + canonicalClassName
                    + "] "
                    + "cannot represent multiple java classes with the same name from different class loaders"
            );
        }

        PainlessClassBuilder existingPainlessClassBuilder = classesToPainlessClassBuilders.get(clazz);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder = new PainlessClassBuilder();

            canonicalClassNamesToClasses.put(canonicalClassName.intern(), clazz);
            classesToPainlessClassBuilders.put(clazz, painlessClassBuilder);
        }

        String javaClassName = clazz.getName();
        String importedCanonicalClassName = javaClassName.substring(javaClassName.lastIndexOf('.') + 1).replace('$', '.');

        if (canonicalClassName.equals(importedCanonicalClassName)) {
            if (importClassName) {
                throw new IllegalArgumentException("must use no_import parameter on class [" + canonicalClassName + "] with no package");
            }
        } else {
            Class<?> importedClass = canonicalClassNamesToClasses.get(importedCanonicalClassName);

            if (importedClass == null) {
                if (importClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException("inconsistent no_import parameter found for class [" + canonicalClassName + "]");
                    }

                    canonicalClassNamesToClasses.put(importedCanonicalClassName.intern(), clazz);
                }
            } else if (importedClass != clazz) {
                throw new IllegalArgumentException(
                    "imported class ["
                        + importedCanonicalClassName
                        + "] cannot represent multiple "
                        + "classes ["
                        + canonicalClassName
                        + "] and ["
                        + typeToCanonicalTypeName(importedClass)
                        + "]"
                );
            } else if (importClassName == false) {
                throw new IllegalArgumentException("inconsistent no_import parameter found for class [" + canonicalClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(
        String targetCanonicalClassName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found"
                    + "for constructor [["
                    + targetCanonicalClassName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + canonicalTypeNameParameter
                        + "] not found "
                        + "for constructor [["
                        + targetCanonicalClassName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]"
                );
            }

            typeParameters.add(typeParameter);
        }

        addPainlessConstructor(targetClass, typeParameters, annotations);
    }

    public void addPainlessConstructor(Class<?> targetClass, List<Class<?>> typeParameters, Map<Class<?>, Object> annotations) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = targetClass.getCanonicalName();
        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found"
                    + "for constructor [["
                    + targetCanonicalClassName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] not found "
                        + "for constructor [["
                        + targetCanonicalClassName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        Constructor<?> javaConstructor;

        try {
            javaConstructor = targetClass.getConstructor(javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException(
                "reflection object not found for constructor "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]",
                nsme
            );
        }

        MethodHandle methodHandle;

        try {
            methodHandle = MethodHandles.publicLookup().in(targetClass).unreflectConstructor(javaConstructor);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException(
                "method handle not found for constructor "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]",
                iae
            );
        }

        MethodType methodType = methodHandle.type();

        String painlessConstructorKey = buildPainlessConstructorKey(typeParametersSize);
        PainlessConstructor existingPainlessConstructor = painlessClassBuilder.constructors.get(painlessConstructorKey);
        PainlessConstructor newPainlessConstructor = new PainlessConstructor(
            javaConstructor,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingPainlessConstructor == null) {
            newPainlessConstructor = painlessConstructorCache.computeIfAbsent(newPainlessConstructor, key -> key);
            painlessClassBuilder.constructors.put(painlessConstructorKey.intern(), newPainlessConstructor);
        } else if (newPainlessConstructor.equals(existingPainlessConstructor) == false) {
            throw new IllegalArgumentException(
                "cannot add constructors with the same arity but are not equivalent for constructors "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] and "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + typesToCanonicalTypeNames(existingPainlessConstructor.typeParameters)
                    + "]"
            );
        }
    }

    public void addPainlessMethod(
        ClassLoader classLoader,
        String targetCanonicalClassName,
        String augmentedCanonicalClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found for method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        Class<?> augmentedClass = null;

        if (augmentedCanonicalClassName != null) {
            try {
                augmentedClass = Class.forName(augmentedCanonicalClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException(
                    "augmented class ["
                        + augmentedCanonicalClassName
                        + "] not found for method "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]",
                    cnfe
                );
            }
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + canonicalTypeNameParameter
                        + "] not found for method "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]"
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException(
                "return type ["
                    + returnCanonicalTypeName
                    + "] not found for method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        addPainlessMethod(targetClass, augmentedClass, methodName, returnType, typeParameters, annotations);
    }

    public void addPainlessMethod(
        Class<?> targetClass,
        Class<?> augmentedClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add method to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found for method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        int typeParametersSize = typeParameters.size();
        int augmentedParameterOffset = augmentedClass == null ? 0 : 1;
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize + augmentedParameterOffset);

        if (augmentedClass != null) {
            javaTypeParameters.add(targetClass);
        }

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] "
                        + "not found for method [["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] not found for method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        Method javaMethod;

        if (augmentedClass == null) {
            try {
                javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException(
                    "reflection object not found for method [["
                        + targetCanonicalClassName
                        + "], "
                        + "["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]",
                    nsme
                );
            }
        } else {
            try {
                javaMethod = augmentedClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));

                if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    throw new IllegalArgumentException(
                        "method [["
                            + targetCanonicalClassName
                            + "], ["
                            + methodName
                            + "], "
                            + typesToCanonicalTypeNames(typeParameters)
                            + "] with augmented class "
                            + "["
                            + typeToCanonicalTypeName(augmentedClass)
                            + "] must be static"
                    );
                }
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException(
                    "reflection object not found for method "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "] "
                        + "with augmented class ["
                        + typeToCanonicalTypeName(augmentedClass)
                        + "]",
                    nsme
                );
            }
        }

        // injections alter the type parameters required for the user to call this method, since some are injected by compiler
        if (annotations.containsKey(InjectConstantAnnotation.class)) {
            int numInjections = ((InjectConstantAnnotation) annotations.get(InjectConstantAnnotation.class)).injects.size();

            if (numInjections > 0) {
                typeParameters.subList(0, numInjections).clear();
            }

            typeParametersSize = typeParameters.size();
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(javaMethod.getReturnType())
                    + "] "
                    + "does not match the specified returned type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] "
                    + "for method [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        MethodHandle methodHandle;

        if (augmentedClass == null) {
            try {
                methodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                    "method handle not found for method "
                        + "[["
                        + targetClass.getCanonicalName()
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]",
                    iae
                );
            }
        } else {
            try {
                methodHandle = MethodHandles.publicLookup().in(augmentedClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                    "method handle not found for method "
                        + "[["
                        + targetClass.getCanonicalName()
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                        + "with augmented class ["
                        + typeToCanonicalTypeName(augmentedClass)
                        + "]",
                    iae
                );
            }
        }

        MethodType methodType = methodHandle.type();
        boolean isStatic = augmentedClass == null && Modifier.isStatic(javaMethod.getModifiers());
        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);
        PainlessMethod existingPainlessMethod = isStatic
            ? painlessClassBuilder.staticMethods.get(painlessMethodKey)
            : painlessClassBuilder.methods.get(painlessMethodKey);
        PainlessMethod newPainlessMethod = new PainlessMethod(
            javaMethod,
            targetClass,
            returnType,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingPainlessMethod == null) {
            newPainlessMethod = painlessMethodCache.computeIfAbsent(newPainlessMethod, key -> key);

            if (isStatic) {
                painlessClassBuilder.staticMethods.put(painlessMethodKey.intern(), newPainlessMethod);
            } else {
                painlessClassBuilder.methods.put(painlessMethodKey.intern(), newPainlessMethod);
            }
        } else if (newPainlessMethod.equals(existingPainlessMethod) == false) {
            throw new IllegalArgumentException(
                "cannot add methods with the same name and arity but are not equivalent for methods "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(returnType)
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] and "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(existingPainlessMethod.returnType)
                    + "], "
                    + typesToCanonicalTypeNames(existingPainlessMethod.typeParameters)
                    + "]"
            );
        }
    }

    public void addPainlessField(String targetCanonicalClassName, String fieldName, String canonicalTypeNameParameter) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(canonicalTypeNameParameter);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found for field "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + fieldName
                    + "], ["
                    + canonicalTypeNameParameter
                    + "]]"
            );
        }

        Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

        if (typeParameter == null) {
            throw new IllegalArgumentException(
                "type parameter ["
                    + canonicalTypeNameParameter
                    + "] not found "
                    + "for field [["
                    + targetCanonicalClassName
                    + "], ["
                    + fieldName
                    + "]"
            );
        }

        addPainlessField(targetClass, fieldName, typeParameter);
    }

    public void addPainlessField(Class<?> targetClass, String fieldName, Class<?> typeParameter) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(typeParameter);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add field to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (FIELD_NAME_PATTERN.matcher(fieldName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid field name [" + fieldName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found for field "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + fieldName
                    + "], ["
                    + typeToCanonicalTypeName(typeParameter)
                    + "]]"
            );
        }

        if (isValidType(typeParameter) == false) {
            throw new IllegalArgumentException(
                "type parameter ["
                    + typeToCanonicalTypeName(typeParameter)
                    + "] not found "
                    + "for field [["
                    + targetCanonicalClassName
                    + "], ["
                    + fieldName
                    + "]"
            );
        }

        Field javaField;

        try {
            javaField = targetClass.getField(fieldName);
        } catch (NoSuchFieldException nsme) {
            throw new IllegalArgumentException(
                "reflection object not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]",
                nsme
            );
        }

        if (javaField.getType() != typeToJavaType(typeParameter)) {
            throw new IllegalArgumentException(
                "type parameter ["
                    + typeToCanonicalTypeName(javaField.getType())
                    + "] "
                    + "does not match the specified type parameter ["
                    + typeToCanonicalTypeName(typeParameter)
                    + "] "
                    + "for field [["
                    + targetCanonicalClassName
                    + "], ["
                    + fieldName
                    + "]"
            );
        }

        MethodHandle methodHandleGetter;

        try {
            methodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException(
                "getter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]"
            );
        }

        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException("static field [[" + targetCanonicalClassName + "], [" + fieldName + "]] must be final");
            }

            PainlessField existingPainlessField = painlessClassBuilder.staticFields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(javaField, typeParameter, methodHandleGetter, null);

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, key -> key);
                painlessClassBuilder.staticFields.put(painlessFieldKey.intern(), newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw new IllegalArgumentException(
                    "cannot add fields with the same name but are not equivalent for fields "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + fieldName
                        + "], ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] and "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + existingPainlessField.javaField.getName()
                        + "], "
                        + typeToCanonicalTypeName(existingPainlessField.typeParameter)
                        + "] "
                        + "with the same name and different type parameters"
                );
            }
        } else {
            MethodHandle methodHandleSetter;

            try {
                methodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                    "setter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]"
                );
            }

            PainlessField existingPainlessField = painlessClassBuilder.fields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(javaField, typeParameter, methodHandleGetter, methodHandleSetter);

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, key -> key);
                painlessClassBuilder.fields.put(painlessFieldKey.intern(), newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw new IllegalArgumentException(
                    "cannot add fields with the same name but are not equivalent for fields "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + fieldName
                        + "], ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] and "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + existingPainlessField.javaField.getName()
                        + "], "
                        + typeToCanonicalTypeName(existingPainlessField.typeParameter)
                        + "] "
                        + "with the same name and different type parameters"
                );
            }
        }
    }

    public void addImportedPainlessMethod(
        ClassLoader classLoader,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass;

        try {
            targetClass = Class.forName(targetJavaClassName, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("class [" + targetJavaClassName + "] not found", cnfe);
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (targetClass == null) {
            throw new IllegalArgumentException(
                "target class ["
                    + targetCanonicalClassName
                    + "] not found for imported method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + canonicalTypeNameParameter
                        + "] not found for imported method "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]"
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException(
                "return type ["
                    + returnCanonicalTypeName
                    + "] not found for imported method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        addImportedPainlessMethod(targetClass, methodName, returnType, typeParameters, annotations);
    }

    public void addImportedPainlessMethod(
        Class<?> targetClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add imported method from reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw new IllegalArgumentException(
                "class ["
                    + targetCanonicalClassName
                    + "] "
                    + "cannot represent multiple java classes with the same name from different class loaders"
            );
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid imported method name [" + methodName + "] for target class [" + targetCanonicalClassName + "]."
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] "
                        + "not found for imported method [["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] not found for imported method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        Method javaMethod;

        try {
            javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException(
                "imported method reflection object [["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] not found",
                nsme
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(javaMethod.getReturnType())
                    + "] "
                    + "does not match the specified returned type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] "
                    + "for imported method [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
            throw new IllegalArgumentException(
                "imported method [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] must be static"
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (painlessMethodKeysToPainlessClassBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("imported method and class binding cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessInstanceBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("imported method and instance binding cannot have the same name [" + methodName + "]");
        }

        MethodHandle methodHandle;

        try {
            methodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException(
                "imported method handle [["
                    + targetClass.getCanonicalName()
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] not found",
                iae
            );
        }

        MethodType methodType = methodHandle.type();

        PainlessMethod existingImportedPainlessMethod = painlessMethodKeysToImportedPainlessMethods.get(painlessMethodKey);
        PainlessMethod newImportedPainlessMethod = new PainlessMethod(
            javaMethod,
            targetClass,
            returnType,
            typeParameters,
            methodHandle,
            methodType,
            annotations
        );

        if (existingImportedPainlessMethod == null) {
            newImportedPainlessMethod = painlessMethodCache.computeIfAbsent(newImportedPainlessMethod, key -> key);
            painlessMethodKeysToImportedPainlessMethods.put(painlessMethodKey.intern(), newImportedPainlessMethod);
        } else if (newImportedPainlessMethod.equals(existingImportedPainlessMethod) == false) {
            throw new IllegalArgumentException(
                "cannot add imported methods with the same name and arity "
                    + "but do not have equivalent methods "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(returnType)
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] and "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(existingImportedPainlessMethod.returnType)
                    + "], "
                    + typesToCanonicalTypeNames(existingImportedPainlessMethod.typeParameters)
                    + "]"
            );
        }
    }

    public void addPainlessClassBinding(
        ClassLoader classLoader,
        String targetJavaClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        Map<Class<?>, Object> annotations
    ) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass;

        try {
            targetClass = Class.forName(targetJavaClassName, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("class [" + targetJavaClassName + "] not found", cnfe);
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + canonicalTypeNameParameter
                        + "] not found for class binding "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]"
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException(
                "return type ["
                    + returnCanonicalTypeName
                    + "] not found for class binding "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        addPainlessClassBinding(targetClass, methodName, returnType, typeParameters, annotations);
    }

    public void addPainlessClassBinding(
        Class<?> targetClass,
        String methodName,
        Class<?> returnType,
        List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations
    ) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add class binding as reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw new IllegalArgumentException(
                "class ["
                    + targetCanonicalClassName
                    + "] "
                    + "cannot represent multiple java classes with the same name from different class loaders"
            );
        }

        Constructor<?>[] javaConstructors = targetClass.getConstructors();
        Constructor<?> javaConstructor = null;

        for (Constructor<?> eachJavaConstructor : javaConstructors) {
            if (eachJavaConstructor.getDeclaringClass() == targetClass) {
                if (javaConstructor != null) {
                    throw new IllegalArgumentException(
                        "class binding [" + targetCanonicalClassName + "] cannot have multiple constructors"
                    );
                }

                javaConstructor = eachJavaConstructor;
            }
        }

        if (javaConstructor == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one constructor");
        }

        int constructorTypeParametersSize = javaConstructor.getParameterCount();

        for (int typeParameterIndex = 0; typeParameterIndex < constructorTypeParametersSize; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] not found "
                        + "for class binding [["
                        + targetCanonicalClassName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            Class<?> javaTypeParameter = javaConstructor.getParameterTypes()[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] not found "
                        + "for class binding [["
                        + targetCanonicalClassName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(javaTypeParameter)
                        + "] "
                        + "does not match the specified type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] "
                        + "for class binding [["
                        + targetClass.getCanonicalName()
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for class binding [" + targetCanonicalClassName + "]."
            );
        }

        Method[] javaMethods = targetClass.getMethods();
        Method javaMethod = null;

        for (Method eachJavaMethod : javaMethods) {
            if (eachJavaMethod.getDeclaringClass() == targetClass) {
                if (javaMethod != null) {
                    throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] cannot have multiple methods");
                }

                javaMethod = eachJavaMethod;
            }
        }

        if (javaMethod == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one method");
        }

        int methodTypeParametersSize = javaMethod.getParameterCount();

        for (int typeParameterIndex = 0; typeParameterIndex < methodTypeParametersSize; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(constructorTypeParametersSize + typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] not found "
                        + "for class binding [["
                        + targetCanonicalClassName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            Class<?> javaTypeParameter = javaMethod.getParameterTypes()[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] not found "
                        + "for class binding [["
                        + targetCanonicalClassName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(javaTypeParameter)
                        + "] "
                        + "does not match the specified type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] "
                        + "for class binding [["
                        + targetClass.getCanonicalName()
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] not found for class binding "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(javaMethod.getReturnType())
                    + "] "
                    + "does not match the specified returned type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] "
                    + "for class binding [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, constructorTypeParametersSize + methodTypeParametersSize);

        if (painlessMethodKeysToImportedPainlessMethods.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("class binding and imported method cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessInstanceBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("class binding and instance binding cannot have the same name [" + methodName + "]");
        }

        if (Modifier.isStatic(javaMethod.getModifiers())) {
            throw new IllegalArgumentException(
                "class binding [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] cannot be static"
            );
        }

        PainlessClassBinding existingPainlessClassBinding = painlessMethodKeysToPainlessClassBindings.get(painlessMethodKey);
        PainlessClassBinding newPainlessClassBinding = new PainlessClassBinding(
            javaConstructor,
            javaMethod,
            returnType,
            typeParameters,
            annotations
        );

        if (existingPainlessClassBinding == null) {
            newPainlessClassBinding = painlessClassBindingCache.computeIfAbsent(newPainlessClassBinding, key -> key);
            painlessMethodKeysToPainlessClassBindings.put(painlessMethodKey.intern(), newPainlessClassBinding);
        } else if (newPainlessClassBinding.equals(existingPainlessClassBinding) == false) {
            throw new IllegalArgumentException(
                "cannot add class bindings with the same name and arity "
                    + "but do not have equivalent methods "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(returnType)
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] and "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(existingPainlessClassBinding.returnType)
                    + "], "
                    + typesToCanonicalTypeNames(existingPainlessClassBinding.typeParameters)
                    + "]"
            );
        }
    }

    public void addPainlessInstanceBinding(
        Object targetInstance,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters
    ) {

        Objects.requireNonNull(targetInstance);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = targetInstance.getClass();
        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + canonicalTypeNameParameter
                        + "] not found for instance binding "
                        + "[["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + canonicalTypeNameParameters
                        + "]"
                );
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException(
                "return type ["
                    + returnCanonicalTypeName
                    + "] not found for class binding "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + canonicalTypeNameParameters
                    + "]"
            );
        }

        addPainlessInstanceBinding(targetInstance, methodName, returnType, typeParameters);
    }

    public void addPainlessInstanceBinding(Object targetInstance, String methodName, Class<?> returnType, List<Class<?>> typeParameters) {
        Objects.requireNonNull(targetInstance);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        Class<?> targetClass = targetInstance.getClass();

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add instance binding as reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        Class<?> existingTargetClass = javaClassNamesToClasses.get(targetClass.getName());

        if (existingTargetClass == null) {
            javaClassNamesToClasses.put(targetClass.getName().intern(), targetClass);
        } else if (existingTargetClass != targetClass) {
            throw new IllegalArgumentException(
                "class ["
                    + targetCanonicalClassName
                    + "] "
                    + "cannot represent multiple java classes with the same name from different class loaders"
            );
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                "invalid method name [" + methodName + "] for instance binding [" + targetCanonicalClassName + "]."
            );
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException(
                    "type parameter ["
                        + typeToCanonicalTypeName(typeParameter)
                        + "] "
                        + "not found for instance binding [["
                        + targetCanonicalClassName
                        + "], ["
                        + methodName
                        + "], "
                        + typesToCanonicalTypeNames(typeParameters)
                        + "]"
                );
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] not found for imported method "
                    + "[["
                    + targetCanonicalClassName
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        Method javaMethod;

        try {
            javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException(
                "instance binding reflection object [["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] not found",
                nsme
            );
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException(
                "return type ["
                    + typeToCanonicalTypeName(javaMethod.getReturnType())
                    + "] "
                    + "does not match the specified returned type ["
                    + typeToCanonicalTypeName(returnType)
                    + "] "
                    + "for instance binding [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "]"
            );
        }

        if (Modifier.isStatic(javaMethod.getModifiers())) {
            throw new IllegalArgumentException(
                "instance binding [["
                    + targetClass.getCanonicalName()
                    + "], ["
                    + methodName
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] cannot be static"
            );
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (painlessMethodKeysToImportedPainlessMethods.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("instance binding and imported method cannot have the same name [" + methodName + "]");
        }

        if (painlessMethodKeysToPainlessClassBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("instance binding and class binding cannot have the same name [" + methodName + "]");
        }

        PainlessInstanceBinding existingPainlessInstanceBinding = painlessMethodKeysToPainlessInstanceBindings.get(painlessMethodKey);
        PainlessInstanceBinding newPainlessInstanceBinding = new PainlessInstanceBinding(
            targetInstance,
            javaMethod,
            returnType,
            typeParameters
        );

        if (existingPainlessInstanceBinding == null) {
            newPainlessInstanceBinding = painlessInstanceBindingCache.computeIfAbsent(newPainlessInstanceBinding, key -> key);
            painlessMethodKeysToPainlessInstanceBindings.put(painlessMethodKey.intern(), newPainlessInstanceBinding);
        } else if (newPainlessInstanceBinding.equals(existingPainlessInstanceBinding) == false) {
            throw new IllegalArgumentException(
                "cannot add instances bindings with the same name and arity "
                    + "but do not have equivalent methods "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(returnType)
                    + "], "
                    + typesToCanonicalTypeNames(typeParameters)
                    + "] and "
                    + "[["
                    + targetCanonicalClassName
                    + "], "
                    + "["
                    + methodName
                    + "], "
                    + "["
                    + typeToCanonicalTypeName(existingPainlessInstanceBinding.returnType)
                    + "], "
                    + typesToCanonicalTypeNames(existingPainlessInstanceBinding.typeParameters)
                    + "]"
            );
        }
    }

    public PainlessLookup build() {
        copyPainlessClassMembers();
        setFunctionalInterfaceMethods();
        generateRuntimeMethods();
        cacheRuntimeHandles();

        Map<Class<?>, PainlessClass> classesToPainlessClasses = new HashMap<>(classesToPainlessClassBuilders.size());

        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            classesToPainlessClasses.put(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue().build());
        }

        if (javaClassNamesToClasses.values().containsAll(canonicalClassNamesToClasses.values()) == false) {
            throw new IllegalArgumentException(
                "the values of java class names to classes " + "must be a superset of the values of canonical class names to classes"
            );
        }

        if (javaClassNamesToClasses.values().containsAll(classesToPainlessClasses.keySet()) == false) {
            throw new IllegalArgumentException(
                "the values of java class names to classes " + "must be a superset of the keys of classes to painless classes"
            );
        }

        if (canonicalClassNamesToClasses.values().containsAll(classesToPainlessClasses.keySet()) == false
            || classesToPainlessClasses.keySet().containsAll(canonicalClassNamesToClasses.values()) == false) {
            throw new IllegalArgumentException(
                "the values of canonical class names to classes " + "must have the same classes as the keys of classes to painless classes"
            );
        }

        return new PainlessLookup(
            javaClassNamesToClasses,
            canonicalClassNamesToClasses,
            classesToPainlessClasses,
            painlessMethodKeysToImportedPainlessMethods,
            painlessMethodKeysToPainlessClassBindings,
            painlessMethodKeysToPainlessInstanceBindings
        );
    }

    private void copyPainlessClassMembers() {
        for (Class<?> parentClass : classesToPainlessClassBuilders.keySet()) {
            copyPainlessInterfaceMembers(parentClass, parentClass);

            Class<?> childClass = parentClass.getSuperclass();

            while (childClass != null) {
                if (classesToPainlessClassBuilders.containsKey(childClass)) {
                    copyPainlessClassMembers(childClass, parentClass);
                }

                copyPainlessInterfaceMembers(childClass, parentClass);
                childClass = childClass.getSuperclass();
            }
        }

        for (Class<?> javaClass : classesToPainlessClassBuilders.keySet()) {
            if (javaClass.isInterface()) {
                copyPainlessClassMembers(Object.class, javaClass);
            }
        }
    }

    private void copyPainlessInterfaceMembers(Class<?> parentClass, Class<?> targetClass) {
        for (Class<?> childClass : parentClass.getInterfaces()) {
            if (classesToPainlessClassBuilders.containsKey(childClass)) {
                copyPainlessClassMembers(childClass, targetClass);
            }

            copyPainlessInterfaceMembers(childClass, targetClass);
        }
    }

    private void copyPainlessClassMembers(Class<?> originalClass, Class<?> targetClass) {
        PainlessClassBuilder originalPainlessClassBuilder = classesToPainlessClassBuilders.get(originalClass);
        PainlessClassBuilder targetPainlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        Objects.requireNonNull(originalPainlessClassBuilder);
        Objects.requireNonNull(targetPainlessClassBuilder);

        for (Map.Entry<String, PainlessMethod> painlessMethodEntry : originalPainlessClassBuilder.methods.entrySet()) {
            String painlessMethodKey = painlessMethodEntry.getKey();
            PainlessMethod newPainlessMethod = painlessMethodEntry.getValue();
            PainlessMethod existingPainlessMethod = targetPainlessClassBuilder.methods.get(painlessMethodKey);

            if (existingPainlessMethod == null
                || existingPainlessMethod.targetClass != newPainlessMethod.targetClass
                    && existingPainlessMethod.targetClass.isAssignableFrom(newPainlessMethod.targetClass)) {
                targetPainlessClassBuilder.methods.put(painlessMethodKey.intern(), newPainlessMethod);
            }
        }

        for (Map.Entry<String, PainlessField> painlessFieldEntry : originalPainlessClassBuilder.fields.entrySet()) {
            String painlessFieldKey = painlessFieldEntry.getKey();
            PainlessField newPainlessField = painlessFieldEntry.getValue();
            PainlessField existingPainlessField = targetPainlessClassBuilder.fields.get(painlessFieldKey);

            if (existingPainlessField == null
                || existingPainlessField.javaField.getDeclaringClass() != newPainlessField.javaField.getDeclaringClass()
                    && existingPainlessField.javaField.getDeclaringClass()
                        .isAssignableFrom(newPainlessField.javaField.getDeclaringClass())) {
                targetPainlessClassBuilder.fields.put(painlessFieldKey.intern(), newPainlessField);
            }
        }
    }

    private void setFunctionalInterfaceMethods() {
        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            setFunctionalInterfaceMethod(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue());
        }
    }

    private void setFunctionalInterfaceMethod(Class<?> targetClass, PainlessClassBuilder painlessClassBuilder) {
        if (targetClass.isInterface()) {
            List<java.lang.reflect.Method> javaMethods = new ArrayList<>();

            for (java.lang.reflect.Method javaMethod : targetClass.getMethods()) {
                if (javaMethod.isDefault() == false && Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    try {
                        Object.class.getMethod(javaMethod.getName(), javaMethod.getParameterTypes());
                    } catch (ReflectiveOperationException roe) {
                        javaMethods.add(javaMethod);
                    }
                }
            }

            if (javaMethods.size() != 1 && targetClass.isAnnotationPresent(FunctionalInterface.class)) {
                throw new IllegalArgumentException(
                    "class ["
                        + typeToCanonicalTypeName(targetClass)
                        + "] "
                        + "is illegally marked as a FunctionalInterface with java methods "
                        + javaMethods
                );
            } else if (javaMethods.size() == 1) {
                java.lang.reflect.Method javaMethod = javaMethods.get(0);
                String painlessMethodKey = buildPainlessMethodKey(javaMethod.getName(), javaMethod.getParameterCount());
                painlessClassBuilder.functionalInterfaceMethod = painlessClassBuilder.methods.get(painlessMethodKey);
            }
        }
    }

    /**
     * Creates a {@link Map} of PainlessMethodKeys to {@link PainlessMethod}s per {@link PainlessClass} stored as
     * {@link PainlessClass#runtimeMethods} identical to {@link PainlessClass#methods} with the exception of generated
     * bridge methods. A generated bridge method is created for each allowlisted method that has at least one parameter
     * with a boxed type to cast from other numeric primitive/boxed types in a symmetric was not handled by
     * {@link MethodHandle#asType(MethodType)}. As an example {@link MethodHandle#asType(MethodType)} legally casts
     * from {@link Integer} to long but not from int to {@link Long}. Generated bridge methods cover the latter case.
     * A generated bridge method replaces the method its a bridge to in the {@link PainlessClass#runtimeMethods}
     * {@link Map}. The {@link PainlessClass#runtimeMethods} {@link Map} is used exclusively to look up methods at
     * run-time resulting from calls with a def type value target.
     */
    private void generateRuntimeMethods() {
        for (PainlessClassBuilder painlessClassBuilder : classesToPainlessClassBuilders.values()) {
            painlessClassBuilder.runtimeMethods.putAll(painlessClassBuilder.methods);

            for (PainlessMethod painlessMethod : painlessClassBuilder.runtimeMethods.values()) {
                for (Class<?> typeParameter : painlessMethod.typeParameters) {
                    if (typeParameter == Byte.class
                        || typeParameter == Short.class
                        || typeParameter == Character.class
                        || typeParameter == Integer.class
                        || typeParameter == Long.class
                        || typeParameter == Float.class
                        || typeParameter == Double.class) {
                        generateBridgeMethod(painlessClassBuilder, painlessMethod);
                    }
                }
            }
        }
    }

    private void generateBridgeMethod(PainlessClassBuilder painlessClassBuilder, PainlessMethod painlessMethod) {
        String painlessMethodKey = buildPainlessMethodKey(painlessMethod.javaMethod.getName(), painlessMethod.typeParameters.size());
        PainlessMethod bridgePainlessMethod = painlessBridgeCache.get(painlessMethod);

        if (bridgePainlessMethod == null) {
            Method javaMethod = painlessMethod.javaMethod;
            boolean isStatic = Modifier.isStatic(painlessMethod.javaMethod.getModifiers());

            int bridgeClassFrames = ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS;
            int bridgeClassAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
            String bridgeClassName = "org/opensearch/painless/Bridge$"
                + javaMethod.getDeclaringClass().getSimpleName()
                + "$"
                + javaMethod.getName();
            ClassWriter bridgeClassWriter = new ClassWriter(bridgeClassFrames);
            bridgeClassWriter.visit(
                WriterConstants.CLASS_VERSION,
                bridgeClassAccess,
                bridgeClassName,
                null,
                OBJECT_TYPE.getInternalName(),
                null
            );

            org.objectweb.asm.commons.Method bridgeConstructorType = new org.objectweb.asm.commons.Method(
                "<init>",
                MethodType.methodType(void.class).toMethodDescriptorString()
            );
            GeneratorAdapter bridgeConstructorWriter = new GeneratorAdapter(
                Opcodes.ASM5,
                bridgeConstructorType,
                bridgeClassWriter.visitMethod(
                    Opcodes.ACC_PRIVATE,
                    bridgeConstructorType.getName(),
                    bridgeConstructorType.getDescriptor(),
                    null,
                    null
                )
            );
            bridgeConstructorWriter.visitCode();
            bridgeConstructorWriter.loadThis();
            bridgeConstructorWriter.invokeConstructor(OBJECT_TYPE, bridgeConstructorType);
            bridgeConstructorWriter.returnValue();
            bridgeConstructorWriter.endMethod();

            int bridgeTypeParameterOffset = isStatic ? 0 : 1;
            List<Class<?>> bridgeTypeParameters = new ArrayList<>(javaMethod.getParameterTypes().length + bridgeTypeParameterOffset);

            if (isStatic == false) {
                bridgeTypeParameters.add(javaMethod.getDeclaringClass());
            }

            for (Class<?> typeParameter : javaMethod.getParameterTypes()) {
                if (typeParameter == Byte.class
                    || typeParameter == Short.class
                    || typeParameter == Character.class
                    || typeParameter == Integer.class
                    || typeParameter == Long.class
                    || typeParameter == Float.class
                    || typeParameter == Double.class) {
                    bridgeTypeParameters.add(Object.class);
                } else {
                    bridgeTypeParameters.add(typeParameter);
                }
            }

            MethodType bridgeMethodType = MethodType.methodType(painlessMethod.returnType, bridgeTypeParameters);
            MethodWriter bridgeMethodWriter = new MethodWriter(
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                new org.objectweb.asm.commons.Method(painlessMethod.javaMethod.getName(), bridgeMethodType.toMethodDescriptorString()),
                bridgeClassWriter,
                null,
                null
            );
            bridgeMethodWriter.visitCode();

            if (isStatic == false) {
                bridgeMethodWriter.loadArg(0);
            }

            final Class<?>[] typeParameters = javaMethod.getParameterTypes();
            for (int typeParameterCount = 0; typeParameterCount < typeParameters.length; ++typeParameterCount) {
                bridgeMethodWriter.loadArg(typeParameterCount + bridgeTypeParameterOffset);
                Class<?> typeParameter = typeParameters[typeParameterCount];

                if (typeParameter == Byte.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_BYTE_IMPLICIT);
                else if (typeParameter == Short.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_SHORT_IMPLICIT);
                else if (typeParameter == Character.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_CHARACTER_IMPLICIT);
                else if (typeParameter == Integer.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_INTEGER_IMPLICIT);
                else if (typeParameter == Long.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_LONG_IMPLICIT);
                else if (typeParameter == Float.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_FLOAT_IMPLICIT);
                else if (typeParameter == Double.class) bridgeMethodWriter.invokeStatic(DEF_UTIL_TYPE, DEF_TO_B_DOUBLE_IMPLICIT);
            }

            bridgeMethodWriter.invokeMethodCall(painlessMethod);
            bridgeMethodWriter.returnValue();
            bridgeMethodWriter.endMethod();

            bridgeClassWriter.visitEnd();

            try {
                @SuppressWarnings("removal")
                BridgeLoader bridgeLoader = AccessController.doPrivileged(new PrivilegedAction<BridgeLoader>() {
                    @Override
                    public BridgeLoader run() {
                        return new BridgeLoader(javaMethod.getDeclaringClass().getClassLoader());
                    }
                });

                Class<?> bridgeClass = bridgeLoader.defineBridge(bridgeClassName.replace('/', '.'), bridgeClassWriter.toByteArray());
                Method bridgeMethod = bridgeClass.getMethod(
                    painlessMethod.javaMethod.getName(),
                    bridgeTypeParameters.toArray(new Class<?>[0])
                );
                MethodHandle bridgeHandle = MethodHandles.publicLookup().in(bridgeClass).unreflect(bridgeClass.getMethods()[0]);
                bridgePainlessMethod = new PainlessMethod(
                    bridgeMethod,
                    bridgeClass,
                    painlessMethod.returnType,
                    bridgeTypeParameters,
                    bridgeHandle,
                    bridgeMethodType,
                    Collections.emptyMap()
                );
                painlessClassBuilder.runtimeMethods.put(painlessMethodKey.intern(), bridgePainlessMethod);
                painlessBridgeCache.put(painlessMethod, bridgePainlessMethod);
            } catch (Exception exception) {
                throw new IllegalStateException(
                    "internal error occurred attempting to generate a bridge method [" + bridgeClassName + "]",
                    exception
                );
            }
        } else {
            painlessClassBuilder.runtimeMethods.put(painlessMethodKey.intern(), bridgePainlessMethod);
        }
    }

    private void cacheRuntimeHandles() {
        for (PainlessClassBuilder painlessClassBuilder : classesToPainlessClassBuilders.values()) {
            cacheRuntimeHandles(painlessClassBuilder);
        }
    }

    private void cacheRuntimeHandles(PainlessClassBuilder painlessClassBuilder) {
        for (Map.Entry<String, PainlessMethod> painlessMethodEntry : painlessClassBuilder.methods.entrySet()) {
            String methodKey = painlessMethodEntry.getKey();
            PainlessMethod painlessMethod = painlessMethodEntry.getValue();
            PainlessMethod bridgePainlessMethod = painlessClassBuilder.runtimeMethods.get(methodKey);
            String methodName = painlessMethod.javaMethod.getName();
            int typeParametersSize = painlessMethod.typeParameters.size();

            if (typeParametersSize == 0
                && methodName.startsWith("get")
                && methodName.length() > 3
                && Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.getterMethodHandles.putIfAbsent(
                    Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4),
                    bridgePainlessMethod.methodHandle
                );
            } else if (typeParametersSize == 0
                && methodName.startsWith("is")
                && methodName.length() > 2
                && Character.isUpperCase(methodName.charAt(2))) {
                    painlessClassBuilder.getterMethodHandles.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3),
                        bridgePainlessMethod.methodHandle
                    );
                } else if (typeParametersSize == 1
                    && methodName.startsWith("set")
                    && methodName.length() > 3
                    && Character.isUpperCase(methodName.charAt(3))) {
                        painlessClassBuilder.setterMethodHandles.putIfAbsent(
                            Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4),
                            bridgePainlessMethod.methodHandle
                        );
                    }
        }

        for (PainlessField painlessField : painlessClassBuilder.fields.values()) {
            painlessClassBuilder.getterMethodHandles.put(painlessField.javaField.getName().intern(), painlessField.getterMethodHandle);
            painlessClassBuilder.setterMethodHandles.put(painlessField.javaField.getName().intern(), painlessField.setterMethodHandle);
        }
    }
}
