# :material-test-tube: Testing the workflows

From the perspective of standard software engineering, we could consider **:octicons-workflow-24: workflows** as
chains of ETL and ML **:octicons-package-dependents-24: tasks**.

These **:octicons-package-dependents-24: tasks** are subjects to standard testing practices.
Let's briefly describe these practices for better understanding of the testing principles.

!!! note

    A more elaborate guidance on the wider topic of application testing could be found
    in [this essay by Ham Vocke](https://martinfowler.com/articles/practical-test-pyramid.html),
    as well as in the must-read "Clean Architecture" :octicons-book-24: book by Robert Cecil Martin.

## :material-pyramid: Testing pyramid in workflows

Testing pyramid is a classical depiction of the testing levels.
This approach sorts the tests by two criteria: :material-cog-counterclockwise: integration and :octicons-zap-24: speed.

At the top of the pyramid there are usually **end-to-end** tests. The end-to-end tests verify that
the whole data product is working as expected, without putting attention to how specific parts operate.
Usually these tests are the hardest to implement and execute, since it requires coordinated work of many teams and services.

Next level towards bottom of the pyramid is occupied by **integration tests**.
Integration tests usually are launched inside the test environment (e.g. a Databricks workspace).

Finally on the bottom level of the pyramid there are unit tests. Usually **unit tests** are the fastest tests,
they cover the business logic of the workflow and verify that application produces expected results.

### :fontawesome-solid-flask-vial: Integration testing of the workflows

In context of **:octicons-workflow-24: workflows** integration tests verify that:

- individual tasks are able to read data from :material-bucket-outline: cloud storages and :material-table: tables
- these tasks are able to consume real-life data
- produced results can be consumed by downstream services, e.g. :material-apache-kafka: streaming queues
- cluster configurations defined for workflows are valid
- tasks are able to match the :material-gauge: SLA with the given amount of resources

For integration tests usually the test workspace is used, however in more enterprise-grade setups it might be
considerable to have a separate integration workspace for such kind of workloads.

!!! hint "Integration testing with `dbx` for Python package-based projects"

    If you're looking for a way to run integration tests for Python package-based projects, check [this doc](../guides/python/integration_tests.md).

### :fontawesome-solid-vial-circle-check: Unit testing of the workflows

**Unit test** is something that can be launched on the developer machine (locally),
without any kind of external dependencies
(e.g. connection to Databricks, cloud infrastructure, cloud storage buckets and containers, 3rd party HTTP services).

Unit tests verify that the **business logic** of the **:octicons-package-dependents-24: tasks** inside workflow is
working as expected.

## :material-file-check: Relation between testing and data quality frameworks

Various data quality frameworks (e.g. [Soda](https://github.com/sodadata/soda-core), [great_expectations](https://greatexpectations.io/), [Deequ](https://github.com/awslabs/deequ) and others)
provide capabilities to test various properties of the data sets.

In context of testing, there might be 2 cases of usage for such frameworks:

* Inside unit-tests as a part of output assertion
* As integration tests as a part of output assertion on a real data

However, the biggest and one of the most important is to run DQ checks as **separate tasks** on a regular basis (e.g. in a :material-timer-sync: scheduled fashion) out of the testing scope.

## :material-note-search: Real-life example

Imagine that you need to develop a streaming pipeline that should do the following:

- read data from a [:material-table: Delta Table](https://www.databricks.com/product/delta-lake-on-databricks)
- apply complex :material-filter: filtering logic
- output result to a :material-apache-kafka: Apache Kafka topic

For your local setup you probably would start from writing an Apache Spark application that mocks the input data and the
output sink. In this application your tests would cover various data inputs, and verify that the outputs are following
the logic of the filtering conditions.

Such filtering conditions are easy to be tested locally, since you just need to mock the potential (expected) data
inputs. In a more profound case a Docker Compose which emulates behaviour of an Apache Kafka instance might be added.

However, such tests are still executed on the local machine (or inside a CI pipeline), without any effect on the
downstream services. It's easy to make changes and have very quick :material-axis-z-rotate-clockwise: development loops.

When the test coverage gets to a good level like 80%+, it's time to move
to the :fontawesome-solid-flask-vial: integration testing part.

During integration tests, the workflow is pushed to the testing environment, and tests verify that:

- Workflow is able to consume the data from a real-life table (e.g. permissions are in-place, table exists)
- Filters are actually applied to the real-life data
- Output structures and content follow the business logic
- Downstream system is able to receive messages (e.g. there are no networking isues)

At this stage it might also make sense to perform some :material-car-brake-low-pressure: load testing, e.g. by actively
populating the table with new messages to figure out if workflow is :material-arrow-expand-all: scaling properly.

## `dbx` in the workflows testing

`dbx` provides convenient interfaces to deploy and launch integration tests on various Databricks workspaces.
Please take a look at the `dbx deploy` and `dbx launch` commands for examples and guidance.
