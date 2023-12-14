# Triage in OpenSearch Core
## What Do We Do With These Labels Anyway?

Across all repositories in opensearch-project, we have a process called “triage.” The goal of this process is to make sure repository owners and maintainers check new issues to understand them. First - we learn if the issue is security related (e.g. CVE), second - we understand if we have enough information to act on the issue, third - we re-label or respond on the issue. During triage we do not work on these issues, but instead just try to get the issue to a next step. Because the OpenSearch core repository (https://github.com/opensearch-project/OpenSearch) has so many different functions with different sets of contributors working on these functions, repo owners decided to treat these functions like separate repositories when triaging to get through the backlog faster and have more focus. There are a few maintainers that do triage in public (see https://meetup.com/OpenSearch to join) and there will be more coming.

Here is an example of the workflow for a new issue in the core repo.

1. Every issue is created with an untriaged label by default.
2. No less than once per week, untriaged issues should be reviewed:
    1. Read through the issue to understand the request.
        1. If the issue does not meet the template requirements, ask the requester for missing details and keep the untriaged label until all the details have been disclosed .
        2. Move to 3 if you have all the details on the associated issue.
3. If the issue does not impact the OpenSearch core repository, then transfer the issue to the appropriate team using the Transfer issue button on the lower right hand corner. If you don’t have the access to transfer, then you can add comment by mentioning [@opensearch-project/admin](https://github.com/orgs/opensearch-project/teams/admin) to do the transfer for you. If not, move to step 4.
4. Assign the labels as per the details of the ticket
    1. Overall request type - bug. enhancement, feature.
    2. Additional context like good first issue, wontfix, question, discuss, rfc ...etc. ([General guidelines for github triaging](https://github.com/opensearch-project/.github/blob/main/RESPONSIBILITIES.md#triage-open-issues))
    3. Make sure to enrich the issue with more details by adding your thoughts, add more context to the issue, or ask the author for more clarification if needed ...etc.
    4. Remove the untriaged label
    5. Add the component level labels based on TABLE 1
5. At any point a maintainer or issue owner can add or remove a label which will help clarify the work that needs to be done. This RFC will also become a markdown file in the OpenSearch repo and we will add a link to the issue templates in the repo so issue creators can reference this process to help it go smoother.


TABLE 1

|Component/Area	|Sub-Area	|Github Label	|Desc	|
|---	|---	|---	|---	|
|Search	|Resiliency\Scale	|Search:Resiliency	|keep search working in spite of failures that may occur	|
|Search	|Query Performance	|Search:Performance	|maintain and improve read query performance	|
|Search	|Query Capabilities	|Search:Query Capabilities	|add new capabilities for querying	|
|Search	|Query Insights	|Search:Query Insights	|capabilities to understand what's happening under the covers in a query	|
|Search	|Aggregations	|Search:Aggregations	|aggregations/facets	|
|Search	|Remote Search	|Search:Remote Search	|using remote storage for search applications	|
|Search	|Search Relevance	|Search:Relevance	|query tuning to improve results (tools, query language, features)	|
|Search	|Searchable Snapshots	|Search:Searchable Snapshots	| |
|Search	|Others\Unknowns	|Search	|catch-all for unclear issues (could break these down, assign multiple labels, add other labels)	|
|Indexing	|Replication	|Indexing:Replication	|moving data throughout the cluster at index time	|
|Indexing	|Performance\Throughput	|Indexing:Performance	|things that make indexing perform better	|
|Indexing	|Others\Unknowns	|Indexing	|catch-all for unclear issues (could break these down, assign multiple labels, add other labels)	|
|Storage	|Storage:Snapshots	|Storage:Snapshots	|	|
|Storage	|Storage:Performance	|Storage:Performance	| |
|Storage	|Storage:Durability	|Storage:Durability	| |
|Storage	|Remote Storage	|Storage:Remote	| |
|Storage	|Others\Unknowns	|Storage	|catch-all for unclear issues (could break these down, assign multiple labels, add other labels)	|
|Cluster Manager	|Cluster Manager	|Cluster Manager	| |
|Extensions	|Extensions	|Extensions	| |
|Release & Build	|Build and Libraries	|Build Libraries & Interfaces	|Make sure the build tasks are useful and own the whole gradle plugin so that packaging and distribution are easy. 	|
|Release & Build	|Upgrades	|	|For example Lucence Upgrades	|
|Release & Build	|Core Plugins	|Plugins	|Plugins within [Plugins](https://github.com/opensearch-project/OpenSearch/tree/main/plugins) like: language analyzers or within [modules](https://github.com/opensearch-project/OpenSearch/tree/main/modules)	|
|Release & Build	|Plugins Framework	|Plugins	|Solves the plugin infrastructure with core.	|
