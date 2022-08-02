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
 * An instance binding represents a method call that stores state. Each instance binding must provide
 * exactly one public method name. The canonical type name parameters provided must match those of the
 * method. The method for an instance binding will target the specified Java instance.
 */
public class AllowlistInstanceBinding extends WhitelistInstanceBinding {
    /**
     * Standard constructor. All values must be not {@code null}.
     */
    public AllowlistInstanceBinding(
        String origin,
        Object targetInstance,
        String methodName,
        String returnCanonicalTypeName,
        List<String> canonicalTypeNameParameters,
        List<Object> painlessAnnotations
    ) {
        super(origin, targetInstance, methodName, returnCanonicalTypeName, canonicalTypeNameParameters, painlessAnnotations);
    }
}
