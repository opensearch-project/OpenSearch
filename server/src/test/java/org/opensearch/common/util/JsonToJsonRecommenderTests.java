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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.util;

import org.opensearch.test.OpenSearchTestCase;

// import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for JsonToJsonRecommender.getRecommendation method.
 * Tests the main entry point for getting JSON transformation recommendations.
 */
public class JsonToJsonRecommenderTests extends OpenSearchTestCase {
    public void testSimpleNestedStructures() throws Exception {
        String inputJson = """
            {
              "level1": {
                "level2": {
                  "level3": {
                    "data": "deep_value"
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "result": {
                "nested": {
                  "info": {
                    "value": "deep_value"
                  }
                }
              }
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "result" : {
                "nested" : {
                  "info" : {
                    "value" : "$.level1.level2.level3.data"
                  }
                }
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "result" : {
                "nested" : {
                  "info" : {
                    "value" : "$.level1.level2.level3.data"
                  }
                }
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testVerbose() throws Exception {
        String inputJson = """
            {
              "level1": {
                "level2": {
                  "level3": {
                    "data": "deep_value"
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "result": {
                "nested": {
                  "info": {
                    "value": "deep_value"
                  }
                }
              }
            }
            """;

        // Test with showDetails = true
        String verbose = JsonToJsonRecommender.getRecommendation(inputJson, outputJson, true).verbose;

        String expectedVerbose = """
            Detailed Field Mapping:
            ------------------------------------------------------------
            | Output Field               | Input Field                 |
            ------------------------------------------------------------
            | $.result.nested.info.value | $.level1.level2.level3.data |
            ------------------------------------------------------------

            Generalized Field Mapping:
            ------------------------------------------------------------
            | Output Field               | Input Field                 |
            ------------------------------------------------------------
            | $.result.nested.info.value | $.level1.level2.level3.data |
            ------------------------------------------------------------""";

        assertEquals("Verbose output should match expected format", expectedVerbose, verbose);

        // Test with showDetails = false should have empty verbose
        String verboseEmpty = JsonToJsonRecommender.getRecommendation(inputJson, outputJson, false).verbose;
        assertEquals("Verbose should be empty when showDetails is false", "", verboseEmpty);
    }

    public void testArrayToArrayWithGeneralized() throws Exception {
        String inputJson = """
            {"numbers": [1, 2, 3, 4, 5]}
            """;

        String outputJson = """
            {"values": [1, 2, 3, 4, 5]}
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "values[0]" : "$.numbers[0]",
              "values[1]" : "$.numbers[1]",
              "values[2]" : "$.numbers[2]",
              "values[3]" : "$.numbers[3]",
              "values[4]" : "$.numbers[4]"
            }""";

        String expectedGeneralizedJson = """
            {
              "values[*]" : "$.numbers[*]"
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testArrayToArrayCannotBeGeneralized() throws Exception {
        String inputJson = """
            {"numbers": [1, 2, 3, 4, 5]}
            """;

        String outputJson = """
            {"values": [1, 2, 3, 4, 6]}
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "values[0]" : "$.numbers[0]",
              "values[1]" : "$.numbers[1]",
              "values[2]" : "$.numbers[2]",
              "values[3]" : "$.numbers[3]"
            }""";

        String expectedGeneralizedJson = """
            {
              "values[0]" : "$.numbers[0]",
              "values[1]" : "$.numbers[1]",
              "values[2]" : "$.numbers[2]",
              "values[3]" : "$.numbers[3]"
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void test1ToN() throws Exception {
        String inputJson = """
            {
                "user": {
                    "id": "u123",
                    "skills": ["Java", "Python", "Go"]
                }
            }
            """;

        String outputJson = """
            {
                "skills" : [
                    {
                        "user_id": "u123",
                        "skill": "Java"
                    },
                    {
                        "user_id": "u123",
                        "skill": "Python"
                    },
                    {
                        "user_id": "u123",
                        "skill": "Go"
                    }
                ]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "skills[0]" : {
                "user_id" : "$.user.id",
                "skill" : "$.user.skills[0]"
              },
              "skills[1]" : {
                "user_id" : "$.user.id",
                "skill" : "$.user.skills[1]"
              },
              "skills[2]" : {
                "user_id" : "$.user.id",
                "skill" : "$.user.skills[2]"
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "skills[*]" : {
                "user_id" : "$.user.id",
                "skill" : "$.user.skills[*]"
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testNTo1() throws Exception {
        String inputJson = """
            [
                {
                    "hits": [
                        {
                            "_source": {
                                "books": { "name": "To Kill a Mockingbird" },
                                "songs": { "name": "Pocketful of Sunshine" }
                            }
                        }
                    ]
                },
                {
                    "hits": [
                        {
                            "_source": {
                                "books": { "name": "Where the Crawdads Sing" },
                                "songs": { "name": "If" }
                            }
                        }
                    ]
                }
            ]
            """;

        String outputJson = """
            {
                "book_name": ["To Kill a Mockingbird", "Where the Crawdads Sing"],
                "song_name": ["Pocketful of Sunshine", "If"]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "book_name[0]" : "$[0].hits[0]._source.books.name",
              "book_name[1]" : "$[1].hits[0]._source.books.name",
              "song_name[0]" : "$[0].hits[0]._source.songs.name",
              "song_name[1]" : "$[1].hits[0]._source.songs.name"
            }""";

        String expectedGeneralizedJson = """
            {
              "book_name[*]" : "$[*].hits[0]._source.books.name",
              "song_name[*]" : "$[*].hits[0]._source.songs.name"
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testComplexArrayToArray() throws Exception {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": {
                  "useful": {
                    "updatedAt": "2020-08-10T14:26:48-07:00",
                    "token": 1134,
                    "items": [
                      {
                        "allocation": 0.2,
                        "team": {
                          "id": 90,
                          "name": "Some Team Name 1",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.9,
                        "team": {
                          "id": 80,
                          "name": "Some Team Name 2",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.1,
                        "team": {
                          "id": 10,
                          "name": "Some Team Name 3",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "allocDetails": [
                {
                  "allocation": 0.2,
                  "team": {
                    "id": 90,
                    "name": "Some Team Name 1"
                  }
                },
                {
                  "allocation": 0.9,
                  "team": {
                    "id": 80,
                    "name": "Some Team Name 2"
                  }
                },
                {
                  "allocation": 0.1,
                  "team": {
                    "id": 10,
                    "name": "Some Team Name 3"
                  }
                }
              ]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "name" : "$.item.name",
              "allocDetails[0]" : {
                "allocation" : "$.item.allocDetails.useful.items[0].allocation",
                "team" : {
                  "id" : "$.item.allocDetails.useful.items[0].team.id",
                  "name" : "$.item.allocDetails.useful.items[0].team.name"
                }
              },
              "allocDetails[1]" : {
                "allocation" : "$.item.allocDetails.useful.items[1].allocation",
                "team" : {
                  "id" : "$.item.allocDetails.useful.items[1].team.id",
                  "name" : "$.item.allocDetails.useful.items[1].team.name"
                }
              },
              "allocDetails[2]" : {
                "allocation" : "$.item.allocDetails.useful.items[2].allocation",
                "team" : {
                  "id" : "$.item.allocDetails.useful.items[2].team.id",
                  "name" : "$.item.allocDetails.useful.items[2].team.name"
                }
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "name" : "$.item.name",
              "allocDetails[*]" : {
                "allocation" : "$.item.allocDetails.useful.items[*].allocation",
                "team" : {
                  "id" : "$.item.allocDetails.useful.items[*].team.id",
                  "name" : "$.item.allocDetails.useful.items[*].team.name"
                }
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testComplexArrayToArrayWithPartialInputGeneralization() throws Exception {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": [
                  {
                    "useful": {
                      "updatedAt": "2020-08-10T14:26:48-07:00",
                      "token": 1134,
                      "items": [
                        {
                          "allocation": 0.2,
                          "team": {
                            "id": 90,
                            "name": "Some Team Name 1",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.9,
                          "team": {
                            "id": 80,
                            "name": "Some Team Name 2",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.1,
                          "team": {
                            "id": 10,
                            "name": "Some Team Name 3",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        }
                      ]
                    }
                  },
                  {
                    "useless": {
                      "aa": "bb"
                    }
                  }
                ]
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "allocDetails": [
                {
                  "allocation": 0.2,
                  "team": {
                    "id": 90,
                    "name": "Some Team Name 1"
                  }
                },
                {
                  "allocation": 0.9,
                  "team": {
                    "id": 80,
                    "name": "Some Team Name 2"
                  }
                },
                {
                  "allocation": 0.1,
                  "team": {
                    "id": 10,
                    "name": "Some Team Name 3"
                  }
                }
              ]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "name" : "$.item.name",
              "allocDetails[0]" : {
                "allocation" : "$.item.allocDetails[0].useful.items[0].allocation",
                "team" : {
                  "id" : "$.item.allocDetails[0].useful.items[0].team.id",
                  "name" : "$.item.allocDetails[0].useful.items[0].team.name"
                }
              },
              "allocDetails[1]" : {
                "allocation" : "$.item.allocDetails[0].useful.items[1].allocation",
                "team" : {
                  "id" : "$.item.allocDetails[0].useful.items[1].team.id",
                  "name" : "$.item.allocDetails[0].useful.items[1].team.name"
                }
              },
              "allocDetails[2]" : {
                "allocation" : "$.item.allocDetails[0].useful.items[2].allocation",
                "team" : {
                  "id" : "$.item.allocDetails[0].useful.items[2].team.id",
                  "name" : "$.item.allocDetails[0].useful.items[2].team.name"
                }
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "name" : "$.item.name",
              "allocDetails[*]" : {
                "allocation" : "$.item.allocDetails[0].useful.items[*].allocation",
                "team" : {
                  "id" : "$.item.allocDetails[0].useful.items[*].team.id",
                  "name" : "$.item.allocDetails[0].useful.items[*].team.name"
                }
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testComplexArrayToArrayWithPartialOutputGeneralization() throws Exception {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": {
                  "useful": {
                    "updatedAt": "2020-08-10T14:26:48-07:00",
                    "token": 1134,
                    "items": [
                      {
                        "allocation": 0.2,
                        "team": {
                          "id": 90,
                          "name": "Some Team Name 1",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.9,
                        "team": {
                          "id": 80,
                          "name": "Some Team Name 2",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      },
                      {
                        "allocation": 0.1,
                        "team": {
                          "id": 10,
                          "name": "Some Team Name 3",
                          "createdAt": "2010-01-19T10:52:52-07:00"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "temp": [
                {
                  "allocDetails": [
                    {
                      "allocation": 0.2,
                      "team": {
                        "id": 90,
                        "name": "Some Team Name 1"
                      }
                    },
                    {
                      "allocation": 0.9,
                      "team": {
                        "id": 80,
                        "name": "Some Team Name 2"
                      }
                    },
                    {
                      "allocation": 0.1,
                      "team": {
                        "id": 10,
                        "name": "Some Team Name 3"
                      }
                    }
                  ]
                },
                {
                  "unuseful": "haha"
                }
              ]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "name" : "$.item.name",
              "temp[0]" : {
                "allocDetails[0]" : {
                  "allocation" : "$.item.allocDetails.useful.items[0].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails.useful.items[0].team.id",
                    "name" : "$.item.allocDetails.useful.items[0].team.name"
                  }
                },
                "allocDetails[1]" : {
                  "allocation" : "$.item.allocDetails.useful.items[1].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails.useful.items[1].team.id",
                    "name" : "$.item.allocDetails.useful.items[1].team.name"
                  }
                },
                "allocDetails[2]" : {
                  "allocation" : "$.item.allocDetails.useful.items[2].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails.useful.items[2].team.id",
                    "name" : "$.item.allocDetails.useful.items[2].team.name"
                  }
                }
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "name" : "$.item.name",
              "temp[0]" : {
                "allocDetails[*]" : {
                  "allocation" : "$.item.allocDetails.useful.items[*].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails.useful.items[*].team.id",
                    "name" : "$.item.allocDetails.useful.items[*].team.name"
                  }
                }
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testComplexArrayToArrayWithPartialOutputAndInputGeneralization() throws Exception {
        String inputJson = """
            {
              "item": {
                "id": "abcde",
                "name": "Champak Kumar",
                "allocDetails": [
                  {
                    "useful": {
                      "updatedAt": "2020-08-10T14:26:48-07:00",
                      "token": 1134,
                      "items": [
                        {
                          "allocation": 0.2,
                          "team": {
                            "id": 90,
                            "name": "Some Team Name 1",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.9,
                          "team": {
                            "id": 80,
                            "name": "Some Team Name 2",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        },
                        {
                          "allocation": 0.1,
                          "team": {
                            "id": 10,
                            "name": "Some Team Name 3",
                            "createdAt": "2010-01-19T10:52:52-07:00"
                          }
                        }
                      ]
                    }
                  },
                  {
                    "useless": {
                      "aa": "bb"
                    }
                  }
                ]
              }
            }
            """;

        String outputJson = """
            {
              "name": "Champak Kumar",
              "temp": [
                {
                  "allocDetails": [
                    {
                      "allocation": 0.2,
                      "team": {
                        "id": 90,
                        "name": "Some Team Name 1"
                      }
                    },
                    {
                      "allocation": 0.9,
                      "team": {
                        "id": 80,
                        "name": "Some Team Name 2"
                      }
                    },
                    {
                      "allocation": 0.1,
                      "team": {
                        "id": 10,
                        "name": "Some Team Name 3"
                      }
                    }
                  ]
                },
                {
                  "allocDetails2": {
                    "allocation": 0.2,
                    "team": {
                      "id": 90,
                      "name": "Some Team Name 1"
                    }
                  }
                },
                {
                  "unuseful": "haha"
                }
              ]
            }
            """;

        JsonToJsonRecommender.MappingOutput output = JsonToJsonRecommender.getRecommendation(inputJson, outputJson);

        // Check if the output matches the expected format
        String expectedDetailedJson = """
            {
              "name" : "$.item.name",
              "temp[0]" : {
                "allocDetails[0]" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[0].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[0].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[0].team.name"
                  }
                },
                "allocDetails[1]" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[1].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[1].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[1].team.name"
                  }
                },
                "allocDetails[2]" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[2].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[2].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[2].team.name"
                  }
                }
              },
              "temp[1]" : {
                "allocDetails2" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[0].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[0].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[0].team.name"
                  }
                }
              }
            }""";

        String expectedGeneralizedJson = """
            {
              "name" : "$.item.name",
              "temp[0]" : {
                "allocDetails[*]" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[*].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[*].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[*].team.name"
                  }
                }
              },
              "temp[1]" : {
                "allocDetails2" : {
                  "allocation" : "$.item.allocDetails[0].useful.items[0].allocation",
                  "team" : {
                    "id" : "$.item.allocDetails[0].useful.items[0].team.id",
                    "name" : "$.item.allocDetails[0].useful.items[0].team.name"
                  }
                }
              }
            }""";

        // Verify that the actual output matches the expected structure
        assertEquals("Detailed JSON should match expected format", expectedDetailedJson, output.detailedJsonPathString);
        assertEquals("Generalized JSON should match expected format", expectedGeneralizedJson, output.generalizedJsonPathString);
    }

    public void testGetRecommendationErrorHandlingForNullInputs() {
        // Test null inputs
        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendation(null, "{}"));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendation("{}", null));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendation(null, "{}", false));

        expectThrows(IllegalArgumentException.class, () -> JsonToJsonRecommender.getRecommendation("{}", null, true));
    }

    public void testGetRecommendationWithInvalidJson() {
        String invalidJson = "{invalid json}";
        String validJson = "{}";

        // Should throw exception for invalid input JSON
        expectThrows(Exception.class, () -> JsonToJsonRecommender.getRecommendation(invalidJson, validJson));

        // Should throw exception for invalid output JSON
        expectThrows(Exception.class, () -> JsonToJsonRecommender.getRecommendation(validJson, invalidJson));
    }
}
