/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fieldcaps;

import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * This tests the FieldCapabilities functionality using the next mapping:
 * ------------------------------------------------------------------------
 * <pre>
 * Indices:
 *    - old_index
 *       Mapping:
 *                {
 *                   "_doc": {
 *                     "properties": {
 *                       "distance": {
 *                         "type": "double"
 *                       },
 *                       "route_length_miles": {
 *                         "type": "alias",
 *                         "path": "distance"
 *                       },
 *                       "playlist": {
 *                         "type": "text"
 *                       },
 *                       "secret_soundtrack": {
 *                         "type": "alias",
 *                         "path": "playlist"
 *                       },
 *                       "old_field": {
 *                         "type": "long"
 *                       },
 *                       "new_field": {
 *                         "type": "alias",
 *                         "path": "old_field"
 *                       }
 *                     }
 *                   }
 *                 }
 *    - new_index
 *       Mapping:
 *          {
 *           "_doc": {
 *             "properties": {
 *               "distance": {
 *                 "type": "text"
 *               },
 *               "route_length_miles": {
 *                 "type": "double"
 *               },
 *               "new_field": {
 *                 "type": "long"
 *               }
 *             }
 *           }
 *         }
 *
 *    - another_index
 *       Mapping:
 *                      {
 *               "_doc": {
 *                 "properties": {
 *                   "distance": {
 *                     "type": "text"
 *                   },
 *                   "route_length_miles": {
 *                     "type": "alias",
 *                     "path": "distance"
 *                   },
 *                   "another_route_length_miles": {
 *                     "type": "alias",
 *                     "path": "distance"
 *                   },
 *                   "new_field": {
 *                     "type": "long"
 *                   }
 *                 }
 *               }
 *             }
 *
 * ----------------------------------------------
 * Example requests:
 * ----------------------------------------------
 *  GET _field_caps?fields=route_length_miles
 *  {
 *   "indices": [
 *     "another_index",
 *     "new_index",
 *     "old_index"
 *   ],
 *   "fields": {
 *     "route_length_miles": {
 *       "double": {
 *         "type": "double",
 *         "searchable": true,
 *         "aggregatable": true,
 *         "aliases": [],
 *         "indices": [
 *           "new_index",
 *           "old_index"
 *         ]
 *       },
 *       "text": {
 *         "type": "text",
 *         "searchable": true,
 *         "aggregatable": false,
 *         "aliases": [],
 *         "indices": [
 *           "another_index"
 *         ]
 *       }
 *     }
 *   }
 * }
 *
 * ----------------------------------------------
 *  GET another_index/_field_caps?fields=*
 *  {
 *   "indices": [
 *     "another_index",
 *   ],
 *   "fields": {
 *     "distance": {
 *       "text": {
 *         "type": "text",
 *         "searchable": true,
 *         "aggregatable": false,
 *         "aliases": ["another_route_length_miles","another_route_length_miles"],
 *         "indices": [
 *           "another_index"
 *         ]
 *       }
 *     }
 *   }
 * }
 * </pre>
 */
public class FieldCapabilitiesIT extends OpenSearchIntegTestCase {

    /**
     * <pre>
     * Indices:
     *    - old_index
     *       Mapping:
     *                {
     *                   "_doc": {
     *                     "properties": {
     *                       "distance": {
     *                         "type": "double"
     *                       },
     *                       "route_length_miles": {
     *                         "type": "alias",
     *                         "path": "distance"
     *                       },
     *                       "playlist": {
     *                         "type": "text"
     *                       },
     *                       "secret_soundtrack": {
     *                         "type": "alias",
     *                         "path": "playlist"
     *                       },
     *                       "old_field": {
     *                         "type": "long"
     *                       },
     *                       "new_field": {
     *                         "type": "alias",
     *                         "path": "old_field"
     *                       }
     *                     }
     *                   }
     *                 }
     *    - new_index
     *       Mapping:
     *          {
     *           "_doc": {
     *             "properties": {
     *               "distance": {
     *                 "type": "text"
     *               },
     *               "route_length_miles": {
     *                 "type": "double"
     *               },
     *               "new_field": {
     *                 "type": "long"
     *               }
     *             }
     *           }
     *         }
     *    - another_index
     *       Mapping:
     *                      {
     *               "_doc": {
     *                 "properties": {
     *                   "distance": {
     *                     "type": "text"
     *                   },
     *                   "route_length_miles": {
     *                     "type": "alias",
     *                     "path": "distance"
     *                   },
     *                   "another_route_length_miles": {
     *                     "type": "alias",
     *                     "path": "distance"
     *                   },
     *                   "new_field": {
     *                     "type": "long"
     *                   }
     *                 }
     *               }
     *             }
     * </pre>
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();

        XContentBuilder oldIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "double")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "alias")
            .field("path", "distance")
            .endObject()
            .startObject("playlist")
            .field("type", "text")
            .endObject()
            .startObject("secret_soundtrack")
            .field("type", "alias")
            .field("path", "playlist")
            .endObject()
            .startObject("old_field")
            .field("type", "long")
            .endObject()
            .startObject("new_field")
            .field("type", "alias")
            .field("path", "old_field")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("old_index").setMapping(oldIndexMapping));

        XContentBuilder newIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "text")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "double")
            .endObject()
            .startObject("new_field")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("new_index").setMapping(newIndexMapping));
        assertAcked(client().admin().indices().prepareAliases().addAlias("new_index", "current"));

        XContentBuilder anotherIndexMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("distance")
            .field("type", "text")
            .endObject()
            .startObject("route_length_miles")
            .field("type", "alias")
            .field("path", "distance")
            .endObject()
            .startObject("another_route_length_miles")
            .field("type", "alias")
            .field("path", "distance")
            .endObject()
            .startObject("new_field")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("another_index").setMapping(anotherIndexMapping));
    }

    public static class FieldFilterPlugin extends Plugin implements MapperPlugin {
        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> field -> !field.equals("playlist");
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FieldFilterPlugin.class);
    }

    /**
     * <pre>
     *  GET _field_caps?fields=route_length_miles,distance
     *  {
     *   "indices": [
     *     "another_index",
     *     "new_index",
     *     "old_index"
     *   ],
     *   "fields": {
     *     "distance": {
     *       "double": {
     *         "type": "double",
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [
     *           "old_index:route_length_miles"
     *         ],
     *         "indices": [
     *           "old_index"
     *         ]
     *       },
     *       "text": {
     *         "type": "text",
     *         "searchable": true,
     *         "aggregatable": false,
     *         "aliases": [
     *           "another_index:another_route_length_miles",
     *           "another_index:route_length_miles"
     *         ],
     *         "indices": [
     *           "another_index",
     *           "new_index"
     *         ]
     *       }
     *     },
     *     "route_length_miles": {
     *       "double": {
     *         "type": "double",
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [],
     *         "indices": [
     *           "new_index",
     *           "old_index"
     *         ]
     *       },
     *       "text": {
     *         "type": "text",
     *         "searchable": true,
     *         "aggregatable": false,
     *         "aliases": [],
     *         "indices": [
     *           "another_index"
     *         ]
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public void testFieldAliasInTwoDifferentIndices() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "route_length_miles").get();

        assertIndices(response, "old_index", "new_index", "another_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(2, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "double",
                false,
                true,
                true,
                new String[] { "old_index:route_length_miles" },
                new String[] { "old_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("double")
        );

        assertTrue(distance.containsKey("text"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "text",
                false,
                true,
                false,
                new String[] { "another_index:another_route_length_miles", "another_index:route_length_miles" },
                new String[] { "another_index", "new_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("text")
        );

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(2, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "route_length_miles",
                "double",
                false,
                true,
                true,
                null,
                new String[] { "new_index", "old_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            routeLength.get("double")
        );
    }

    /**
     * <pre>
     *  GET _field_caps?fields=*
     * {
     *   "indices": [
     *     "another_index",
     *     "new_index",
     *     "old_index"
     *   ],
     *   "fields": {
     *     "distance": {
     *       "double": {
     *         "type": "double",
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [
     *           "old_index:route_length_miles"
     *         ],
     *         "indices": [
     *           "old_index"
     *         ]
     *       },
     *       "text": {
     *         "type": "text",
     *         "searchable": true,
     *         "aggregatable": false,
     *         "aliases": [
     *           "another_index:another_route_length_miles",
     *           "another_index:route_length_miles"
     *         ],
     *         "indices": [
     *           "another_index",
     *           "new_index"
     *         ]
     *       }
     *     },
     *     "old_field": {
     *       "long": {
     *         "type": "long",
     *         "searchable": true,
     *         "aggregatable": true
     *       }
     *     },
     *     "route_length_miles": {
     *       "double": {
     *         "type": "double",
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [],
     *         "indices": [
     *           "new_index",
     *           "old_index"
     *         ]
     *       },
     *       "text": {
     *         "type": "text",
     *         "searchable": true,
     *         "aggregatable": false,
     *         "aliases": [],
     *         "indices": [
     *           "another_index"
     *         ]
     *       }
     *     },
     *     "new_field": {
     *       "long": {
     *         "type": "long",
     *         "searchable": true,
     *         "aggregatable": true
     *       }
     *     },
     *     "another_route_length_miles": {
     *       "text": {
     *         "type": "text",
     *         "searchable": true,
     *         "aggregatable": false
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public void testAllFieldInAllIndices() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("*").get();

        assertIndices(response, "old_index", "new_index", "another_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("old_field"));
        assertTrue(response.get().containsKey("route_length_miles"));
        assertTrue(response.get().containsKey("new_field"));
        assertTrue(response.get().containsKey("another_route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(2, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "double",
                false,
                true,
                true,
                new String[] { "old_index:route_length_miles" },
                new String[] { "old_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("double")
        );

        assertTrue(distance.containsKey("text"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "text",
                false,
                true,
                false,
                new String[] { "another_index:another_route_length_miles", "another_index:route_length_miles" },
                new String[] { "another_index", "new_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("text")
        );

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(2, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "route_length_miles",
                "double",
                false,
                true,
                true,
                null,
                new String[] { "new_index", "old_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            routeLength.get("double")
        );
    }

    /**
     * <pre>
     *  GET old_index/_field_caps?fields=route_length_miles
     *  {
     *   "indices": [
     *     "old_index"
     *   ],
     *   "fields": {
     *     "route_length_miles": {
     *       "double": {
     *         "type": "double",
     *         "searchable": true,
     *         "aggregatable": true
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public void testGetOnlyAliasFiled() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("old_index").setFields("route_length_miles").get();

        assertIndices(response, "old_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> route_length_miles = response.getField("route_length_miles");
        assertEquals(1, route_length_miles.size());

        assertTrue(route_length_miles.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "route_length_miles",
                "double",
                true,
                true,
                true,
                null,
                null,
                null,
                null,
                new String[] { "old_index" },
                Collections.emptyMap()
            ),
            route_length_miles.get("double")
        );
    }

    /**
     * <pre>
     *  GET old_index/_field_caps?fields=distance,route_length_miles
     *  {
     *   "indices": [
     *     "old_index"
     *   ],
     *   "fields": {
     *     "distance": {
     *       "double": {
     *         "type": "double",
     *         "alias": false,
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [
     *           "old_index:route_length_miles"
     *         ]
     *       }
     *     },
     *     "route_length_miles": {
     *       "double": {
     *         "type": "double",
     *         "alias": true,
     *         "searchable": true,
     *         "aggregatable": true,
     *         "aliases": [],
     *         "alias_indices": [
     *           "old_index"
     *         ]
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    public void testFieldAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("old_index").setFields("distance", "route_length_miles").get();

        assertIndices(response, "old_index");
        // Ensure the response has entries for both requested fields.
        assertTrue(response.get().containsKey("distance"));
        assertTrue(response.get().containsKey("route_length_miles"));

        // Check the capabilities for the 'distance' field.
        Map<String, FieldCapabilities> distance = response.getField("distance");
        assertEquals(1, distance.size());

        assertTrue(distance.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "distance",
                "double",
                false,
                true,
                true,
                new String[] { "old_index:route_length_miles" },
                null,
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            distance.get("double")
        );

        // Check the capabilities for the 'route_length_miles' alias.
        Map<String, FieldCapabilities> routeLength = response.getField("route_length_miles");
        assertEquals(1, routeLength.size());

        assertTrue(routeLength.containsKey("double"));
        assertEquals(
            new FieldCapabilities(
                "route_length_miles",
                "double",
                true,
                true,
                true,
                null,
                null,
                null,
                null,
                new String[] { "old_index" },
                Collections.emptyMap()
            ),
            routeLength.get("double")
        );
    }

    public void testFieldAliasWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("route*").get();

        assertIndices(response, "old_index", "new_index", "another_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFiltering() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("secret-soundtrack", "route_length_miles").get();
        assertIndices(response, "old_index", "new_index", "another_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("route_length_miles"));
    }

    public void testFieldAliasFilteringWithWildcard() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("distance", "secret*").get();
        assertIndices(response, "old_index", "new_index", "another_index");
        assertEquals(1, response.get().size());
        assertTrue(response.get().containsKey("distance"));
    }

    public void testWithUnmapped() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps().setFields("new_field", "old_field").setIncludeUnmapped(true).get();
        assertIndices(response, "old_index", "new_index", "another_index");

        assertEquals(2, response.get().size());
        assertTrue(response.get().containsKey("old_field"));

        Map<String, FieldCapabilities> oldField = response.getField("old_field");
        assertEquals(2, oldField.size());

        assertTrue(oldField.containsKey("long"));
        assertEquals(
            new FieldCapabilities(
                "old_field",
                "long",
                false,
                true,
                true,
                new String[] { "old_index:new_field" },
                new String[] { "old_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            oldField.get("long")
        );

        assertTrue(oldField.containsKey("unmapped"));
        assertEquals(
            new FieldCapabilities(
                "old_field",
                "unmapped",
                false,
                false,
                false,
                null,
                new String[] { "another_index", "new_index" },
                null,
                null,
                null,
                Collections.emptyMap()
            ),
            oldField.get("unmapped")
        );

        Map<String, FieldCapabilities> newField = response.getField("new_field");
        assertEquals(1, newField.size());

        assertTrue(newField.containsKey("long"));
        assertEquals(
            new FieldCapabilities("new_field", "long", false, true, true, null, null, null, null, null, Collections.emptyMap()),
            newField.get("long")
        );
    }

    public void testWithIndexAlias() {
        FieldCapabilitiesResponse response = client().prepareFieldCaps("current").setFields("*").get();
        assertIndices(response, "new_index");

        FieldCapabilitiesResponse response1 = client().prepareFieldCaps("current", "old_index").setFields("*").get();
        assertIndices(response1, "old_index", "new_index");
        FieldCapabilitiesResponse response2 = client().prepareFieldCaps("current", "old_index", "new_index").setFields("*").get();
        assertEquals(response1, response2);
    }

    public void testWithIndexFilter() throws InterruptedException {
        assertAcked(prepareCreate("index-1").setMapping("timestamp", "type=date", "field1", "type=keyword"));
        assertAcked(prepareCreate("index-2").setMapping("timestamp", "type=date", "field1", "type=long"));

        List<IndexRequestBuilder> reqs = new ArrayList<>();
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2015-07-08"));
        reqs.add(client().prepareIndex("index-1").setSource("timestamp", "2018-07-08"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2019-10-12"));
        reqs.add(client().prepareIndex("index-2").setSource("timestamp", "2020-07-08"));
        indexRandom(true, reqs);

        FieldCapabilitiesResponse response = client().prepareFieldCaps("index-*").setFields("*").get();
        assertIndices(response, "index-1", "index-2");
        Map<String, FieldCapabilities> newField = response.getField("field1");
        assertEquals(2, newField.size());
        assertTrue(newField.containsKey("long"));
        assertTrue(newField.containsKey("keyword"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").gte("2019-11-01"))
            .get();
        assertIndices(response, "index-2");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("long"));

        response = client().prepareFieldCaps("index-*")
            .setFields("*")
            .setIndexFilter(QueryBuilders.rangeQuery("timestamp").lte("2017-01-01"))
            .get();
        assertIndices(response, "index-1");
        newField = response.getField("field1");
        assertEquals(1, newField.size());
        assertTrue(newField.containsKey("keyword"));
    }

    private void assertIndices(FieldCapabilitiesResponse response, String... indices) {
        assertNotNull(response.getIndices());
        Arrays.sort(indices);
        Arrays.sort(response.getIndices());
        assertArrayEquals(indices, response.getIndices());
    }
}
