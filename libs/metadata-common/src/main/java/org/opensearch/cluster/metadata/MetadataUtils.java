/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.applicationtemplates.SystemTemplateMetadata;

import java.util.Optional;

/**
 * Metadata Utilities.
 *
 * @opensearch.internal
 */
public class MetadataUtils {

    private MetadataUtils() {}

    public static final String TEMPLATE_TYPE_KEY = "_type";

    public static boolean isSystemTemplate(ComponentTemplate componentTemplate) {
        return Optional.ofNullable(componentTemplate)
            .map(ComponentTemplate::metadata)
            .map(md -> md.get(TEMPLATE_TYPE_KEY))
            .filter(ob -> SystemTemplateMetadata.COMPONENT_TEMPLATE_TYPE.equals(ob.toString()))
            .isPresent();
    }
}
