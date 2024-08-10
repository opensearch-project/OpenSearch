# README: Running Performance Benchmarks on Pull Requests

## Overview

`benchmark-pull-request` GitHub Actions workflow is designed to automatically run performance benchmarks on a pull request when a specific comment is made on the pull request. This ensures that performance benchmarks are consistently and accurately applied to code changes, helping maintain the performance standards of the repository.

## Workflow Trigger

The workflow is triggered when a new comment is created on a pull request. Specifically, it checks for the presence of the `"run-benchmark-test"` keyword in the comment body. If this keyword is detected, the workflow proceeds to run the performance benchmarks.

## Key Steps in the Workflow

1. **Check Comment Format and Configuration:**
    - Validates the format of the comment to ensure it contains the required `"run-benchmark-test"` keyword and is in json format.
    - Extracts the benchmark configuration ID from the comment and verifies if it exists in the `benchmark-config.json` file.
    - Checks if the extracted configuration ID is supported for the current OpenSearch major version.

2. **Post Invalid Format Comment:**
    - If the comment format is invalid or the configuration ID is not supported, a comment is posted on the pull request indicating the problem, and the workflow fails.

3. **Manual Approval (if necessary):**
    - Fetches the list of approvers from the `.github/CODEOWNERS` file.
    - If the commenter is not one of the maintainers, a manual approval request is created. The workflow pauses until an approver approves or denies the benchmark run by commenting appropriate word on the issue.
    - The issue for approval request is auto-closed once the approver is done adding appropriate comment

4. **Build and Assemble OpenSearch:**
    - Builds and assembles (x64-linux tar) the OpenSearch distribution from the pull request code changes.

5. **Upload to S3:**
    - Configures AWS credentials and uploads the assembled OpenSearch distribution to an S3 bucket for further use in benchmarking.
    - The S3 bucket is fronted by cloudfront to only allow downloads.
    - The lifecycle policy on the S3 bucket will delete the uploaded artifacts after 30-days.

6. **Trigger Jenkins Workflow:**
    - Triggers a Jenkins workflow to run the benchmark tests using a webhook token.

7. **Update Pull Request with Job URL:**
    - Posts a comment on the pull request with the URL of the Jenkins job. The final benchmark results will be posted once the job completes.
    - To learn about how benchmark job works see https://github.com/opensearch-project/opensearch-build/tree/main/src/test_workflow#benchmarking-tests

## How to Use This Workflow

1. **Ensure `benchmark-config.json` is Up-to-Date:**
    - The `benchmark-config.json` file should contain valid benchmark configurations with supported major versions and cluster-benchmark configurations.

2. **Add the Workflow to Your Repository:**
    - Save the workflow YAML file (typically named `benchmark.yml`) in the `.github/workflows` directory of your repository.

3. **Make a Comment to Trigger the Workflow:**
    - On any pull request issue, make a comment containing the keyword `"run-benchmark-test"` along with the configuration ID. For example:
      ```json
      {"run-benchmark-test": "id_1"}
      ```

4. **Monitor Workflow Progress:**
    - The workflow will validate the comment, check for approval (if necessary), build the OpenSearch distribution, and trigger the Jenkins job.
    - A comment will be posted on the pull request with the URL of the Jenkins job. You can monitor the progress and final results there as well.

## Example Comment Format

To run the benchmark with configuration ID `id_1`, post the following comment on the pull request issue:
```json
{"run-benchmark-test": "id_1"}
```

## How to add a new benchmark configuration

The benchmark-config.json file accepts the following schema.
```json
{
  "id_<number>": {
    "description": "Short description of the configuration",
    "supported_major_versions": ["2", "3"],
    "cluster-benchmark-configs": {
      "SINGLE_NODE_CLUSTER": "Use single node cluster for benchmarking, accepted values are \"true\" or \"false\"",
      "MIN_DISTRIBUTION": "Use OpenSearch min distribution, should always be \"true\"",
      "MANAGER_NODE_COUNT": "For multi-node cluster tests, number of cluster manager nodes, empty value defaults to 3.",
      "DATA_NODE_COUNT": "For multi-node cluster tests, number of data nodes, empty value defaults to 2.",
      "DATA_INSTANCE_TYPE": "EC2 instance type for data node, empty defaults to r5.xlarge.",
      "DATA_NODE_STORAGE": "Data node ebs block storage size, empty value defaults to 100Gb",
      "JVM_SYS_PROPS": "A comma-separated list of key=value pairs that will be added to jvm.options as JVM system properties",
      "ADDITIONAL_CONFIG": "Additional space delimited opensearch.yml config parameters. e.g., `search.concurrent_segment_search.enabled:true`",
      "TEST_WORKLOAD": "The workload name from OpenSearch Benchmark Workloads. https://github.com/opensearch-project/opensearch-benchmark-workloads. Default is nyc_taxis",
      "WORKLOAD_PARAMS": "With this parameter you can inject variables into workloads, e.g.{\"number_of_replicas\":\"0\",\"number_of_shards\":\"3\"}. See https://opensearch.org/docs/latest/benchmark/reference/commands/command-flags/#workload-params",
      "EXCLUDE_TASKS": "Defines a comma-separated list of test procedure tasks not to run. e.g. type:search, see https://opensearch.org/docs/latest/benchmark/reference/commands/command-flags/#exclude-tasks",
      "INCLUDE_TASKS": "Defines a comma-separated list of test procedure tasks to run. By default, all tasks listed in a test procedure array are run. See https://opensearch.org/docs/latest/benchmark/reference/commands/command-flags/#include-tasks",
      "TEST_PROCEDURE": "Defines a test procedure to use. e.g., `append-no-conflicts,significant-text`. Uses default if none provided. See https://opensearch.org/docs/latest/benchmark/reference/commands/command-flags/#test-procedure",
      "CAPTURE_NODE_STAT": "Enable opensearch-benchmark node-stats telemetry to capture system level metrics like cpu, jvm etc., see https://opensearch.org/docs/latest/benchmark/reference/telemetry/#node-stats"
    },
    "cluster_configuration": {
      "size": "Single-Node/Multi-Node",
      "data_instance_config": "data-instance-config, e.g., 4vCPU, 32G Mem, 16G Heap"
    }
  }
}
```
To add a new test configuration that are suitable to your changes please create a new PR to add desired cluster and benchmark configurations.

## How to compare my results against baseline?

Apart from just running benchmarks the user will also be interested in how their change is performing against current OpenSearch distribution with the exactly same cluster and benchmark configurations.
The user can refer to https://s12d.com/basline-dashboards (WIP) to access baseline data for their workload, this data is generated by our nightly benchmark runs on latest build distribution artifacts for 3.0 and 2.x.
In the future, we will add the [compare](https://opensearch.org/docs/latest/benchmark/reference/commands/compare/) feature of opensearch-benchmark to run comparison and publish data on the PR as well.

## Notes

- Ensure all required secrets (e.g., `GITHUB_TOKEN`, `UPLOAD_ARCHIVE_ARTIFACT_ROLE`, `ARCHIVE_ARTIFACT_BUCKET_NAME`, `JENKINS_PR_BENCHMARK_GENERIC_WEBHOOK_TOKEN`) are properly set in the repository secrets.
- The `CODEOWNERS` file should list the GitHub usernames of approvers for the benchmark process.

By following these instructions, repository maintainers can ensure consistent and automated performance benchmarking for all code changes introduced via pull requests.

