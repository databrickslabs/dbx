# :material-rename-box: Named properties

With `dbx` you can use name-based properties instead of providing ids in the [:material-file-code: deployment file](../reference/deployment.md).

The following properties are supported:

-   :material-state-machine: `existing_cluster_name` will be automatically replaced with `existing_cluster_id`
-   :fontawesome-solid-microchip: `new_cluster.instance_pool_name` will be automatically replaced with `new_cluster.instance_pool_id`
-   :fontawesome-solid-microchip: `new_cluster.driver_instance_pool_name` will be automatically replaced with `new_cluster.driver_instance_pool_id`
-   :material-aws: `new_cluster.aws_attributes.instance_profile_name` will be automatically replaced with `new_cluster.aws_attributes.instance_profile_arn`

By this simplification, you don't need to look-up for these id-based properties, you can simply provide the names.

!!! warning

    `dbx` will automatically check if the provided name exists and is unique, and if it's doesn't or it's non-unique you'll get an exception.
