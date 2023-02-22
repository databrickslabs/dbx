# :material-format-header-increase: Custom headers support

<img src="https://img.shields.io/badge/available%20since-0.8.8-green?style=for-the-badge" alt="Available since 0.8.8"/>

In 0.8.8 `dbx` introduces support for custom headers during command execution.

The two main modes use either environment variables or additional config variables added to the `.databrickscfg` file.

Through the following three environment variables:

| Environment variable            | `.databrickscfg` variable       | Description                                                  |
|---------------------------------|---------------------------------|--------------------------------------------------------------|
| `AZURE_SERVICE_PRINCIPAL_TOKEN` | `azure_service_principal_token` | An Azure AD token generated on behalf of a Service Principal |
| `WORKSPACE_ID`                  | `workspace_id`                  | The Azure Resource ID of the target workspace                |
| `ORG_ID`                        | `org_id`                        | The Databricks workspace ID of the target workspace          |

By setting these variables, each `dbx` command execution will provide the following three headers to the `ApiClient` in
addition to the predefined headers (i.e. `Authorization, Content-Type, User-Agent`):

- `X-Databricks-Azure-SP-Management-Token`
- `X-Databricks-Azure-Workspace-Resource-Id`
- `X-Databricks-Org-Id`

Another option is to directly provide these properties via `--header/-H` argument.
