- [Search Pipelines](#search-pipelines)
  - [Architecture](#architecture)
  - [Search Processors](#search-processors)
    - [Creating a Search Processor](#creating-a-search-processor)
    - [Creating a Pipeline](#creating-a-search-pipeline)

# Search Pipelines

This README briefly covers the two types of search processors, explains how you can use them to create search pipelines, and walks through the creation of a new processor.

## Architecture

Search pipelines allow cluster operators to create and reuse [components](#search-processors) to transform search queries and results.

With search pipelines, the operator can combine multiple [search processors](#search-processors) to create a transform which acts on the search request and/or search response.

The primary benefit of search pipelines is that they live entirely inside OpenSearch and therefore require less development time. Further, search pipelines support standard APIs for accessing metrics and disaster recovery.

## Search Processors

You can create many search pipelines by combining search processors in various orders. There are two types of search processors:

1. search request processors which transform a request _before_ it is executed;
2. search response processors which transform the output of a request _after_ it is executed.

Currently, there are two search request and two search response processors.

### Creating a search processor

New search processors can be created by following these steps:

1. Create a new class in `org.opensearch.search.pipeline.common`, this class will hold your new processor and should include whether it is a request or response processor. For example, a response processor which deleted a target field could be called `DeleteFieldResponseProcessor`.

2. Make the class extend the generic `AbstractProcessor` class as well as implement either the `SearchRequestProcessor` or `SearchResponseProcessor` class depending on what type of processor it is. In the `DeleteFieldResponseProcessor` example, this would look like:


  public class DeleteFieldResponseProcessor extends AbstractProcessor implements SearchResponseProcessor

3. Create the main functionality of your processor and implement the methods required by the implemented interface. This will be `SearchRequest processRequest(SearchRequest request) throws Exception;` for a search request processor or `SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception;` for a search response processor.

For the example field `DeleteFieldResponseProcessor`, this will look like:

```
@Override
public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
boolean foundField = false;

    SearchHit[] hits = response.getHits().getHits();
    for (SearchHit hit : hits) {
       
      // Process each hit as desired 
      }

      if (hit.hasSource()) {
        
                // Change hit source if needed 
        );

        Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
        if (sourceAsMap.containsKey(field)) {
          
                // Handle source as map 
        }
      }

      if (!foundField && !ignoreMissing) {
        
            // Handle error scenarios 
      }
    }

    return response;
}
```

4. Create a factory to encapsulate the behavior of the class so that processors can be handled uniformly.

5. Add the newly added search processor to the `SearchPieplineCommonModulePlugin` getter for the corresponding processor type.

6. After creating a search processor, the operator can use it in their search pipeline.

To use the new processor, make sure the cluster is reloaded and that the new processor is accessible.

The new processor should show when calling `GET /_nodes/search_pipelines`.

If the new processor is shown in the cURL response, the new processor should be available for use in a search pipeline.

## Creating a Search Pipeline

To create a search pipeline, you must create an ordered list of search processors in the OpenSearch cluster.

An example creation request is:

```
PUT /_search/pipeline/my_pipeline 
{
  "request_processors": [
    {
      "filter_query" : {
        "tag" : "tag1",
        "description" : "This processor is going to restrict to publicly visible documents",
        "query" : {
          "term": {
            "visibility": "public"
          }
        }
      }
    }
  ],
  "response_processors": [
    {
      "rename_field": {
        "field": "message",
        "target_field": "notification"
      }
    }
  ]
}
```
