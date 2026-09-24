/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

/** Tests for {@link MappedFieldType#isSearchableForFieldCaps(IndexSettings)}. */
public class MappedFieldTypeFieldCapsTests extends OpenSearchTestCase {

    private static IndexSettings normalIndex() {
        return IndexSettingsModule.newIndexSettings("normal", Settings.EMPTY);
    }

    private static IndexSettings pluggableIndex() {
        return IndexSettingsModule.newIndexSettings(
            "pluggable",
            Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true).build()
        );
    }

    private static NumberFieldMapper.NumberFieldType numberFieldType(boolean indexed, boolean hasDocValues) {
        return new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            indexed,
            false,
            hasDocValues,
            false,
            true,
            null,
            Collections.emptyMap()
        );
    }

    private static DateFieldMapper.DateFieldType dateFieldType(boolean indexed, boolean hasDocValues) {
        return new DateFieldMapper.DateFieldType(
            "date",
            indexed,
            false,
            hasDocValues,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }

    private static IpFieldMapper.IpFieldType ipFieldType(boolean indexed, boolean hasDocValues) {
        return new IpFieldMapper.IpFieldType("ip", indexed, false, hasDocValues, null, Collections.emptyMap());
    }

    /** Types that do not override the method are unaffected on any index. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testBaseImplementationDelegatesToIsSearchable() {
        for (IndexSettings indexSettings : new IndexSettings[] { normalIndex(), pluggableIndex() }) {
            MappedFieldType indexedKeyword = new KeywordFieldMapper.KeywordFieldType("kw", true, true, Collections.emptyMap());
            assertTrue(indexedKeyword.isSearchableForFieldCaps(indexSettings));

            MappedFieldType unindexedKeyword = new KeywordFieldMapper.KeywordFieldType("kw", false, true, Collections.emptyMap());
            assertFalse(unindexedKeyword.isSearchableForFieldCaps(indexSettings));
        }
    }

    public void testOverriddenTypesUnchangedOnNormalIndex() {
        IndexSettings normal = normalIndex();

        assertTrue(numberFieldType(true, true).isSearchableForFieldCaps(normal));
        assertFalse(numberFieldType(false, true).isSearchableForFieldCaps(normal));

        assertTrue(dateFieldType(true, true).isSearchableForFieldCaps(normal));
        assertFalse(dateFieldType(false, true).isSearchableForFieldCaps(normal));

        assertTrue(ipFieldType(true, true).isSearchableForFieldCaps(normal));
        assertFalse(ipFieldType(false, true).isSearchableForFieldCaps(normal));

        assertTrue(new BooleanFieldMapper.BooleanFieldType("bool", true, true).isSearchableForFieldCaps(normal));
        assertFalse(new BooleanFieldMapper.BooleanFieldType("bool", false, true).isSearchableForFieldCaps(normal));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOverriddenTypesSearchableFromDocValuesOnPluggableIndex() {
        IndexSettings pluggable = pluggableIndex();
        assertTrue(pluggable.isPluggableDataFormatEnabled());

        assertTrue(numberFieldType(false, true).isSearchableForFieldCaps(pluggable));
        assertTrue(dateFieldType(false, true).isSearchableForFieldCaps(pluggable));
        assertTrue(ipFieldType(false, true).isSearchableForFieldCaps(pluggable));
        assertTrue(new BooleanFieldMapper.BooleanFieldType("bool", false, true).isSearchableForFieldCaps(pluggable));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOverriddenTypesNotSearchableWithoutDocValuesOnPluggableIndex() {
        IndexSettings pluggable = pluggableIndex();

        assertFalse(numberFieldType(false, false).isSearchableForFieldCaps(pluggable));
        assertFalse(dateFieldType(false, false).isSearchableForFieldCaps(pluggable));
        assertFalse(ipFieldType(false, false).isSearchableForFieldCaps(pluggable));
        assertFalse(new BooleanFieldMapper.BooleanFieldType("bool", false, false).isSearchableForFieldCaps(pluggable));
    }

    /** Query planning must keep seeing no search index. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIsSearchableItselfIsUnchanged() {
        IndexSettings pluggable = pluggableIndex();

        MappedFieldType number = numberFieldType(false, true);
        assertTrue(number.isSearchableForFieldCaps(pluggable));
        assertFalse(number.isSearchable());

        MappedFieldType date = dateFieldType(false, true);
        assertTrue(date.isSearchableForFieldCaps(pluggable));
        assertFalse(date.isSearchable());

        MappedFieldType ip = ipFieldType(false, true);
        assertTrue(ip.isSearchableForFieldCaps(pluggable));
        assertFalse(ip.isSearchable());

        MappedFieldType bool = new BooleanFieldMapper.BooleanFieldType("bool", false, true);
        assertTrue(bool.isSearchableForFieldCaps(pluggable));
        assertFalse(bool.isSearchable());
    }

    public void testNullIndexSettingsFallsBackToIsSearchable() {
        assertFalse(numberFieldType(false, true).isSearchableForFieldCaps(null));
        assertTrue(numberFieldType(true, true).isSearchableForFieldCaps(null));
    }
}
