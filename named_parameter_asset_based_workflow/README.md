## Demo named parameters issue with asset-based workflows
```
dbx deploy --assets-only named_parameter_asset_based_workflow
```

```
dbx launch test_named_parameters_asset_based_workflows --from-assets
--parameters='[{"task_key": "etl", "named_parameters": {"test_param_1": 1, "test_param": "overriden"}}]'
```
