/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.Numbers;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.geometry.utils.Geohash;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class DerivedSourceTests extends DerivedSourceTestCase {
    // tests for text field
    public void testTextFieldEnableStore() throws IOException {
        final String fieldValue = randomAlphaOfLength(10);
        testOneField(
            b -> b.field("type", "text").field("store", true),
            fieldValue,
            source -> { assertEquals(fieldValue, source.get(FIELD_NAME)); }
        );
    }

    public void testTextFieldEnableStoreWithMultiValues() throws IOException {
        final String oneValue = randomAlphaOfLength(10);
        final String[] fieldValue1 = new String[] { oneValue, oneValue };
        testOneField(b -> b.field("type", "text").field("store", true), fieldValue1, source -> {
            ArrayList<Object> values = (ArrayList<Object>) source.get(FIELD_NAME);
            assertEquals(fieldValue1.length, values.size());
            for (Object value : values) {
                assertEquals(oneValue, value);
            }
        });

        final int numberOfValues = randomIntBetween(2, 10);
        final String[] fieldValue2 = new String[numberOfValues];
        for (int i = 0; i < numberOfValues; i++) {
            fieldValue2[i] = randomAlphaOfLength(10);
        }
        testOneField(b -> b.field("type", "text").field("store", true), fieldValue2, source -> {
            ArrayList<Object> values = (ArrayList<Object>) source.get(FIELD_NAME);
            assertEquals(fieldValue2.length, values.size());
            for (int i = 0; i < values.size(); i++) {
                assertEquals(fieldValue2[i], values.get(i));
            }
        });
    }

    public void testTextFieldEnableDocValuesWithMultiFields() throws IOException {
        final String fieldValue = randomAlphaOfLength(10);
        final boolean storeKeyword = randomBoolean();
        testOneField(
            b -> b.field("type", "text")
                .field("store", false)
                .startObject("fields")
                .startObject("keyword")
                .field("type", "keyword")
                .field("store", storeKeyword)
                .field("doc_values", !storeKeyword)
                .endObject()
                .endObject(),
            fieldValue,
            source -> {
                assertEquals(fieldValue, source.get(FIELD_NAME));
            }
        );
    }

    private void testStringField(String type, boolean onlyDocValues) throws IOException {
        final String fieldValue = randomAlphaOfLength(10);
        final boolean isStored = randomBoolean();
        testOneField(
            b -> b.field("type", type).field("store", isStored).field("doc_values", onlyDocValues || !isStored),
            fieldValue,
            source -> {
                assertEquals(fieldValue, source.get(FIELD_NAME));
            }
        );
    }

    private void testStringFieldEnableStoreWithMultiValues(String type) throws IOException {
        final String oneValue = randomAlphaOfLength(10);
        final String[] fieldValue1 = new String[] { oneValue, oneValue };
        testOneField(b -> b.field("type", type).field("store", true).field("doc_values", false), fieldValue1, source -> {
            ArrayList<Object> values = (ArrayList<Object>) source.get(FIELD_NAME);
            assertEquals(fieldValue1.length, values.size());
            for (Object value : values) {
                assertEquals(oneValue, value);
            }
        });
        final int numberOfValues = randomIntBetween(2, 10);
        final Object[] fieldValue2 = new String[numberOfValues];
        for (int i = 0; i < numberOfValues; i++) {
            fieldValue2[i] = randomAlphaOfLength(10);
        }
        testOneField(b -> b.field("type", type).field("store", true).field("doc_values", false), fieldValue2, source -> {
            ArrayList<Object> values = (ArrayList<Object>) source.get(FIELD_NAME);
            assertEquals(fieldValue2.length, values.size());
            for (int i = 0; i < values.size(); i++) {
                assertEquals(fieldValue2[i], values.get(i));
            }
        });
    }

    private void testStringFieldEnableDocValuesWithMultiValues(String type) throws IOException {
        final String oneValue = randomAlphaOfLength(10);
        final String[] fieldValue1 = new String[] { oneValue, oneValue };
        testOneField(b -> b.field("type", type).field("store", false).field("doc_values", true), fieldValue1, source -> {
            Object value = source.get(FIELD_NAME);
            assertEquals(oneValue, value);
        });
        final int numberOfValues = randomIntBetween(2, 10);
        final String[] fieldValue2 = new String[numberOfValues];
        for (int i = 0; i < numberOfValues; i++) {
            fieldValue2[i] = randomAlphaOfLength(10);
        }
        testOneField(b -> b.field("type", type).field("store", false).field("doc_values", true), fieldValue2, source -> {
            ArrayList<Object> values = (ArrayList<Object>) source.get(FIELD_NAME);
            assertEquals(fieldValue2.length, values.size());
            Arrays.sort(fieldValue2);
            for (int i = 0; i < values.size(); i++) {
                assertEquals(fieldValue2[i], values.get(i));
            }
        });
    }

    // tests for keyword field
    public void testKeywordField() throws IOException {
        testStringField("keyword", false);
    }

    public void testKeywordFieldEnableStoreWithMultiValues() throws IOException {
        testStringFieldEnableStoreWithMultiValues("keyword");
    }

    public void testKeywordFieldEnableDocValuesWithMultiValues() throws IOException {
        testStringFieldEnableDocValuesWithMultiValues("keyword");
    }

    // tests for wildcard field
    public void testWildcardField() throws IOException {
        testStringField("wildcard", true);
    }

    public void testWildcardFieldEnableDocValuesWithMultiValues() throws IOException {
        testStringFieldEnableDocValuesWithMultiValues("wildcard");
    }

    // tests for number field
    public void testIntegerFieldWithSingleValue() throws IOException {
        final CheckedBiConsumer<String, Long, IOException> integerTypeTester = (type, value) -> {
            testOneField(b -> b.field("type", type), value, source -> {
                assertEquals(value, Long.valueOf(((Number) source.get(FIELD_NAME)).longValue()));
            });
        };

        // byte field
        integerTypeTester.accept("byte", (long) randomByte());

        // short field
        integerTypeTester.accept("short", (long) randomShort());

        // integer field
        integerTypeTester.accept("integer", (long) randomInt());

        // long field
        integerTypeTester.accept("long", randomLong());

        // unsigned long field
        final BigInteger unsignedLong = randomUnsignedLong();
        testOneField(b -> b.field("type", "unsigned_long"), unsignedLong, source -> {
            assertEquals(unsignedLong, Numbers.toUnsignedBigInteger(((Number) source.get(FIELD_NAME)).longValue()));
        });
    }

    public void testIntegerFieldWithMultiValues() throws IOException {
        final CheckedBiConsumer<String, Long[], IOException> integerTypeTester = (type, values) -> {
            testOneField(b -> b.field("type", type), values, source -> {
                long[] actualValues = ((ArrayList<Object>) source.get(FIELD_NAME)).stream()
                    .mapToLong(obj -> ((Number) obj).longValue())
                    .toArray();
                assertEquals(values.length, actualValues.length);
                Arrays.sort(values);
                for (int i = 0; i < values.length; i++) {
                    assertEquals((long) values[i], actualValues[i]);
                }
            });
        };

        final Long[] fieldValues = new Long[randomIntBetween(2, 10)];
        // byte field
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = (long) randomByte();
        }
        integerTypeTester.accept("byte", fieldValues);

        // short field
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = (long) randomShort();
        }
        integerTypeTester.accept("short", fieldValues);

        // integer field
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = (long) randomInt();
        }
        integerTypeTester.accept("integer", fieldValues);

        // long field
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = randomLong();
        }
        integerTypeTester.accept("long", fieldValues);

        // unsigned long field
        final BigInteger[] unsignedLongValues = new BigInteger[fieldValues.length];
        for (int i = 0; i < fieldValues.length; i++) {
            unsignedLongValues[i] = randomUnsignedLong();
        }

        testOneField(b -> b.field("type", "unsigned_long"), unsignedLongValues, source -> {
            // map parsed by json can be Long or BigInteger (may include other Number types), so we need to first convert to Number then
            // BigInteger
            BigInteger[] actualValues = ((ArrayList<Number>) source.get(FIELD_NAME)).stream()
                .map(n -> Numbers.toUnsignedBigInteger(n.longValue()))
                .toArray(BigInteger[]::new);
            assertEquals(unsignedLongValues.length, actualValues.length);
            Arrays.sort(unsignedLongValues);
            assertArrayEquals(unsignedLongValues, actualValues);
        });
    }

    // tests for boolean field
    public void testBooleanFieldWithSingleValue() throws IOException {
        final boolean booleanValue = randomBoolean();
        final Object fieldValue = randomBoolean() ? booleanValue : new Boolean[] { booleanValue };
        testOneField(b -> b.field("type", "boolean"), fieldValue, source -> { assertEquals(booleanValue, source.get(FIELD_NAME)); });
    }

    public void testBooleanFieldWithMultiValues() throws IOException {
        final Boolean[] fieldValues = new Boolean[randomIntBetween(2, 10)];
        for (int i = 0; i < fieldValues.length; i++) {
            fieldValues[i] = randomBoolean();
        }
        testOneField(b -> b.field("type", "boolean"), fieldValues, source -> {
            ArrayList<Boolean> actualValues = ((ArrayList<Boolean>) source.get(FIELD_NAME));
            assertEquals(fieldValues.length, actualValues.size());
            Arrays.sort(fieldValues);
            for (int i = 0; i < actualValues.size(); i++) {
                assertEquals(fieldValues[i], actualValues.get(i));
            }
            assertEquals(fieldValues.length, actualValues.size());
            Arrays.sort(fieldValues);
            for (int i = 0; i < actualValues.size(); i++) {
                assertEquals(fieldValues[i], actualValues.get(i));
            }
        });
    }

    // tests for date field
    public void testDateFieldWithSingleValue() throws IOException {
        final String dateValue = "2024-11-11T11:11:11.111Z";
        final long dateValueInMillis = 1731323471111L;
        final Object stringFormat = randomBoolean() ? dateValue : new String[] { dateValue };
        final Object longFormat = randomBoolean() ? dateValueInMillis : new Long[] { dateValueInMillis };
        testOneField(
            b -> b.field("type", "date").field("format", "strict_date_time||epoch_millis"),
            randomBoolean() ? stringFormat : longFormat,
            source -> {
                assertEquals(dateValue, source.get(FIELD_NAME));
            }
        );
    }

    public void testDateFieldWithMultiValues() throws IOException {
        final String[] dateValues = new String[randomIntBetween(2, 10)];
        Arrays.fill(dateValues, "2024-11-11T11:11:11.111Z");
        Object fieldValue = dateValues;
        if (randomBoolean()) {
            fieldValue = new Long[dateValues.length];
            Arrays.fill((Long[]) fieldValue, 1731323471111L);
        }
        testOneField(b -> b.field("type", "date").field("format", "strict_date_time||epoch_millis"), fieldValue, source -> {
            String[] actualValues = ((ArrayList<String>) source.get(FIELD_NAME)).toArray(String[]::new);
            assertEquals(dateValues.length, actualValues.length);
            assertArrayEquals(dateValues, actualValues);
        });

        final Long[] longFormat = new Long[dateValues.length];
        final String[] stringFormat = new String[dateValues.length];
        for (int i = 0; i < longFormat.length; i++) {
            longFormat[i] = randomLongBetween(1704067200000L, 1731323471111L);
            stringFormat[i] = Instant.ofEpochMilli(longFormat[i]).toString();
        }
        testOneField(
            b -> b.field("type", "date").field("format", "strict_date_time||epoch_millis"),
            randomBoolean() ? longFormat : stringFormat,
            source -> {
                String[] actualValues = ((ArrayList<String>) source.get(FIELD_NAME)).toArray(String[]::new);
                assertEquals(longFormat.length, actualValues.length);
                Arrays.sort(stringFormat);
                assertArrayEquals(stringFormat, actualValues);
            }
        );
    }

    // tests for ip field
    public void testIpFieldWithSingleValue() throws IOException {
        final InetAddress inetAddress = randomIp(randomBoolean());
        final String ipValue = NetworkAddress.format(inetAddress);
        final Object fieldValue = randomBoolean() ? ipValue : new String[] { ipValue };
        testOneField(
            b -> b.field("type", "ip"),
            fieldValue,
            source -> { assertEquals(NetworkAddress.format(inetAddress), source.get(FIELD_NAME)); }
        );
    }

    public void testIpFieldWithMultiValues() throws IOException {
        final InetAddress[] inetAddresses = new InetAddress[randomIntBetween(2, 10)];
        Arrays.fill(inetAddresses, randomIp(randomBoolean()));
        final String[] ipValues = new String[inetAddresses.length];
        Arrays.fill(ipValues, NetworkAddress.format(inetAddresses[0]));
        testOneField(b -> b.field("type", "ip"), ipValues, source -> { assertEquals(ipValues[0], source.get(FIELD_NAME)); });

        final byte[][] ipPoints = new byte[inetAddresses.length][];
        for (int i = 0; i < inetAddresses.length; i++) {
            inetAddresses[i] = randomIp(randomBoolean());
            ipValues[i] = NetworkAddress.format(inetAddresses[i]);
            ipPoints[i] = InetAddressPoint.encode(inetAddresses[i]);
        }
        Arrays.sort(ipPoints, Arrays::compareUnsigned);
        final String[] expectedIps = new String[ipPoints.length];
        for (int i = 0; i < ipPoints.length; i++) {
            expectedIps[i] = NetworkAddress.format(InetAddressPoint.decode(ipPoints[i]));
        }
        testOneField(b -> b.field("type", "ip"), ipValues, source -> {
            String[] actualValues = ((ArrayList<String>) source.get(FIELD_NAME)).toArray(String[]::new);
            assertEquals(ipValues.length, actualValues.length);
            Arrays.sort(ipValues);
            assertArrayEquals(expectedIps, actualValues);
        });
    }

    // tests for geo_point field
    private static final double TOLERANCE = 1E-5D;

    public void testGeoPointFieldWithSingleValue() throws IOException {
        final Object geoPoint = randomFrom("POINT (2 3)", Geohash.stringEncode(2, 3), "3,2", new Double[] { 2d, 3d });
        final Object fieldValue = randomBoolean() ? geoPoint : new Object[] { geoPoint };
        testOneField(b -> b.field("type", "geo_point"), fieldValue, source -> {
            final Map<String, Double> hashedGeoPoint = (HashMap<String, Double>) source.get(FIELD_NAME);
            assert (hashedGeoPoint.containsKey("lat"));
            assert (hashedGeoPoint.containsKey("lon"));
            final GeoPoint point = new GeoPoint(hashedGeoPoint.get("lat"), hashedGeoPoint.get("lon"));
            assertEquals(2, point.lon(), TOLERANCE);
            assertEquals(3, point.lat(), TOLERANCE);
        });
    }

    public void testGeoPointFieldWithMultiValues() throws IOException {
        final Object[] geoPoints = new Object[randomIntBetween(3, 10)];
        for (int i = 0; i < geoPoints.length; i++) {
            geoPoints[i] = randomFrom("POINT (2 3)", Geohash.stringEncode(2, 3), "3,2", new double[] { 2, 3 });
        }
        testOneField(b -> b.field("type", "geo_point"), geoPoints, source -> {
            final ArrayList<Map<String, Double>> hashedPoints = (ArrayList<Map<String, Double>>) source.get(FIELD_NAME);
            final GeoPoint[] points = hashedPoints.stream()
                .map(entry -> new GeoPoint(entry.get("lat"), entry.get("lon")))
                .toArray(GeoPoint[]::new);
            for (var point : points) {
                assertEquals(2, point.lon(), TOLERANCE);
                assertEquals(3, point.lat(), TOLERANCE);
            }
        });
    }
}
