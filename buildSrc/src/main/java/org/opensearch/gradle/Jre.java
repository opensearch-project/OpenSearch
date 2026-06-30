/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.model.ObjectFactory;

public class Jre extends JavaVariant {
    Jre(String name, Configuration configuration, ObjectFactory objectFactory) {
        super(name, configuration, objectFactory);
    }
}
