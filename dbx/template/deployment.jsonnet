{
    "dbfs": {
        # this is a dictionary with local files to be downloaded to dbfs
        "package": "dist/package.whl"
    },
    "jobs": [
        # this is a list with jobs to be created/updated during deployment
        # please take a look on Databricks Jobs API for a reference
        # you can reference dependent objects via $["dbfs"]["<name-from-dbfs>"]
        {
            "name": "sample",
            "new_cluster": {},
            "libraries": [
                # this is an example of self-referencing object
                {"whl": $["dbfs"]["package"]}
            ]
        }
    ]
}