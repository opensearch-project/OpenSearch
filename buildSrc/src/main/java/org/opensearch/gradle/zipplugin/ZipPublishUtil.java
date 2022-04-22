/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.gradle.zipplugin;

import org.gradle.api.Project;

class ZipPublishUtil {

    static String capitalize(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    static String getProperty(String name, Project project) {
        if (project.hasProperty(name)) {
            Object property = project.property(name);
            if (property != null) {
                return property.toString();
            }
        }
        return null;
    }

}
