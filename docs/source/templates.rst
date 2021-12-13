Templates
=========

:code:`dbx` provides a set of templated projects for quickstart development on the Databricks platform.

You can easily generate a project by using :code:`dbx init` command.

The list of supported languages and frameworks is described below:

* CI solutions:
    * without CI solution (default option)
    * GitHub Actions (:code:`--with-github-actions`)
    * Azure DevOps (:code:`--with-azure-devops`)
    * GitLab    (:code:`--with-gitlab`)
* Languages (this parameter is required):
    * Python (:code:`--language=python`)
    * Scala: (:code:`--language=scala`). When Scala is chosen, you'll need to choose on of the options for the project build tool:
        * sbt-based (:code:`--sbt-based`)
        * maven-based (:code:`--maven-based`)

