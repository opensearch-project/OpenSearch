/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.util.Collection;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;

/**
 *
 * Registers Python as a plugin.
 *
 * PythonModulePlugin is a plugin for OpenSearch that adds support for Python as a scripting language.
 * This class extends the {@link Plugin} class and implements the {@link ScriptPlugin} interface.
 *
 * <p>
 * This plugin allows users to write and execute scripts in Python within OpenSearch.
 * </p>
 *
 */
public class PythonModulePlugin extends Plugin implements ScriptPlugin {
    /**
     * Constructor for PythonModulePlugin.
     */
    public PythonModulePlugin() {
        // Implement the relevant Plugin Interfaces here

    }

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new PythonScriptEngine();
    }
}
