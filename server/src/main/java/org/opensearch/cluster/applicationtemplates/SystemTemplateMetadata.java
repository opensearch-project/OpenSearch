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

    /**
     * Gets the metadata using fully qualified name for the template
     * @param fullyQualifiedName (e.g. @abc_template@logs@1)
     * @return Metadata object based on name
     */
    public static SystemTemplateMetadata fromComponentTemplate(String fullyQualifiedName) {
        assert fullyQualifiedName.length() > DELIMITER.length() * 3 + 2 + COMPONENT_TEMPLATE_TYPE.length()
            : "System template name must have all defined components";
        assert (DELIMITER + fullyQualifiedName.substring(1, fullyQualifiedName.indexOf(DELIMITER, 1))).equals(COMPONENT_TEMPLATE_TYPE);

        return new SystemTemplateMetadata(
            Long.parseLong(fullyQualifiedName.substring(fullyQualifiedName.lastIndexOf(DELIMITER) + 1)),
            COMPONENT_TEMPLATE_TYPE,
            fullyQualifiedName.substring(fullyQualifiedName.indexOf(DELIMITER, 2) + 1, fullyQualifiedName.lastIndexOf(DELIMITER))
        );
    }

    public static SystemTemplateMetadata fromComponentTemplateInfo(String name, long version) {
        return new SystemTemplateMetadata(version, COMPONENT_TEMPLATE_TYPE, name);
    }

    public final String fullyQualifiedName() {
        return type + DELIMITER + name + DELIMITER + version;
    }
}
