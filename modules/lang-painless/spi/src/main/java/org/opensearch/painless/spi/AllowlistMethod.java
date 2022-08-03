/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless.spi;

import java.util.List;

/**
 * Method represents the equivalent of a Java method available as an allowlisted class method
 * within Painless. Methods for Painless classes may be accessed exactly as methods for Java classes
 * are using the '.' operator on an existing class variable/field. Painless classes may have multiple
 * methods with the same name as long as they comply with arity overloading described in
 * {@link AllowlistClass}.
 *
 * Classes may also have additional methods that are not part of the Java class the class represents -
 * these are known as augmented methods. An augmented method can be added to a class as a part of any
 * Java class as long as the method is static and the first parameter of the method is the Java class
 * represented by the class. Note that the augmented method's parent Java class does not need to be
 * allowlisted.
 */
public class AllowlistMethod extends WhitelistMethod {
    /**
     * Standard constructor. All values must be not {@code null} with the exception of
     * augmentedCanonicalClassName; augmentedCanonicalClassName will be {@code null} unless the method
     * is augmented as described in the class documentation.
     */
    public AllowlistMethod(
        String origin,
        String augmentedCanonicalClassName,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        List<Object> painlessAnnotations
    ) {
        super(origin, augmentedCanonicalClassName, methodName, returnCanonicalTypeName, canonicalTypeNameParameters, painlessAnnotations);
    }
}
