# :fontawesome-solid-users-gear: Permissions management

`dbx` supports permissions management both for Jobs API 2.0 and Jobs API 2.1.

!!! tip

    You can find the full specification for Permissions API [here](https://docs.databricks.com/dev-tools/api/latest/permissions.html).


## :material-tag-outline: For Jobs API 2.0

To manage permissions for Jobs API 2.0, provide the following payload at the workflow level:

```yaml
environments:
  default:
    workflows:
      - name": "job-v2.0"
        permissions:
          ## here goes payload compliant with Permissions API
          access_control_list:
            - user_name: "some_user@example.com"
              permission_level: "IS_OWNER"
            - group_name: "some-user-group"
              permission_level: "CAN_VIEW"
```



## :material-tag-multiple: For Jobs API 2.1

To manage permissions for Jobs API 2.1, provide the following payload at the workflow level:

```yaml
environments:
  default:
    workflows:
      - name": "job-v2.0"
        access_control_list:
          - user_name: "some_user@example.com"
            permission_level: "IS_OWNER"
          - group_name: "some-user-group"
            permission_level: "CAN_VIEW"
```

Please note that in both cases (v2.0 and v2.1) the permissions **must be exhaustive**.
It means that per each job at least the job owner shall be specified (even if it's already specified in the UI).
