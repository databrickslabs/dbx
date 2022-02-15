Jinja2 Support: Environment variables, logic and loops
=============================================================

:code:`dbx` supports `Jinja2 <https://jinja.palletsprojects.com/en/3.0.x/api/>`_ rendering for JSON and YAML based configurations.
This allows you to use environment variables in the deployment, add variable-based conditions, `Jinja filters <https://jinja.palletsprojects.com/en/3.0.x/templates/#filters>`_ and for loops to make your deployment more flexible for CI pipelines.

.. tabs::

   .. tab:: JSON.J2

      .. literalinclude:: ../../tests/deployment-configs/jinja-example.json.j2
         :language: JINJA2

   .. tab:: YAML.J2

      .. literalinclude:: ../../tests/deployment-configs/jinja-example.yaml.j2
         :language: JINJA2
