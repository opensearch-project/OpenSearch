/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

/**
 * Metadata information about a template available in a template repository.
 */
public class SystemTemplateInfo {

    private final long version;
    private final String type;
    private final String name;

    private static final String DELIMITER = "@";

    public static final String COMPONENT_TEMPLATE_TYPE = "@abc_template";

    public SystemTemplateInfo(long version, String type, String name) {
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

    public static SystemTemplateInfo fromComponentTemplate(String fullyQualifiedName) {
        return new SystemTemplateInfo(Long.parseLong(fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf(DELIMITER))), COMPONENT_TEMPLATE_TYPE, fullyQualifiedName.substring(0, fullyQualifiedName.lastIndexOf(DELIMITER)));
    }

    public static SystemTemplateInfo createComponentTemplateInfo(String name, long version) {
        return new SystemTemplateInfo(version, COMPONENT_TEMPLATE_TYPE, name);
    }

    public final String fullyQualifiedName() {
        return name + DELIMITER + version;
    }
}
