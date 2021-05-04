/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.settings;

/**
 * A generic setting upgrader between two settings of the same type.
 * Typically used to rename settings between different versions of plugins.
 * 
 * For example, the following code upgrades {@code num_retries} to {@code retry_count}.
 *
 * <pre>
 * public class MyPluginSettings {
 *  
 *  // new
 *  public static final Setting&#60;Integer&#62; RETRY_COUNT = Setting.intSetting(
 *      "retry_count", 3, Setting.Property.NodeScope);
 * 
 *  // old
 *  public static final Setting&#60;Integer&#62; NUM_RETRIES = RETRY_COUNT.withKey(
 *      "num_retries");
 * }
 * 
 * public class MyPlugin extends Plugin implements ExtensiblePlugin {
 * 
 *  &#64;Override
 *  public List&#60;Setting&#60;?&#62;&#62; getSettings() {
 *      List&#60;Setting&#60;?&#62;&#62; settingList = new ArrayList&#60;&#62;();
 *      settingList.add(MyPluginSettings.RETRY_COUNT); // new
 *      settingList.add(MyPluginSettings.NUM_RETRIES); // old
 *      return settingList;
 *  }
 * 
 *  &#64;Override
 *  public List&#60;Setting&#60;?&#62;&#62; getSettingUpgraders() {
 *      List&#60;SettingUpgrader&#60;?&#62;&#62; settingUpgraders = new ArrayList&#60;&#62;();
 *      settingUpgraders.add(new GenericSettingUpgrader&#60;&#62;(
 *          MyPluginSettings.NUM_RETRIES, // old
 *          MyPluginSettings.RETRY_COUNT  // new
 *      ));
 *      return settingUpgraders;
 *  }
 * 
 * }
 * }</pre>
 *  
 * @param <T> setting type
 */
public class GenericSettingUpgrader<T> implements SettingUpgrader<T> {
    protected final Setting<T> from;
    protected final Setting<T> to;

    public GenericSettingUpgrader(Setting<T> from, Setting<T> to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public Setting<T> getSetting() {
        return this.from;
    }

    @Override
    public String getKey(final String key) {
        return to.getKey();
    }
}
