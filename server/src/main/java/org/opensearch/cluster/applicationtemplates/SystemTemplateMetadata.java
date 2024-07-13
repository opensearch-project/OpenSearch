/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Metadata information about a template available in a template repository.
 */
@ExperimentalApi
public class SystemTemplateMetadata {

    private final long version;
    private final String type;
    private final String name;

    private static final String DELIMITER = "@";

    public static final String COMPONENT_TEMPLATE_TYPE = "@abc_template";

    public SystemTemplateMetadata(long version, String type, String name) {
        this.version = version;
        this.type = type;
        this.name = name;
    }

    public String type() {
        return type;
    }

    public String name() {
        return name;
    }

    public long version() {
        return version;
    }

    public static SystemTemplateMetadata fromComponentTemplate(String fullyQualifiedName) {
        return new SystemTemplateMetadata(Long.parseLong(fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf(DELIMITER))), COMPONENT_TEMPLATE_TYPE, fullyQualifiedName.substring(0, fullyQualifiedName.lastIndexOf(DELIMITER)));
    }

    public static SystemTemplateMetadata createComponentTemplateInfo(String name, long version) {
        return new SystemTemplateMetadata(version, COMPONENT_TEMPLATE_TYPE, name);
    }

    public final String fullyQualifiedName() {
        return name + DELIMITER + version;
    }
}
