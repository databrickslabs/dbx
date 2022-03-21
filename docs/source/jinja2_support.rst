Jinja2 Support: Environment variables, logic and loops
=============================================================

Since version 0.4.1 :code:`dbx` supports `Jinja2 <https://jinja.palletsprojects.com/en/3.0.x/api/>`_ rendering for JSON and YAML based configurations.
This allows you to use environment variables in the deployment, add variable-based conditions, `Jinja filters <https://jinja.palletsprojects.com/en/3.0.x/templates/#filters>`_ and for loops to make your deployment more flexible for CI pipelines.

To add Jinja2 support to your deployment file, please add postfix :code:`.j2` to the name of your deployment file, for example :code:`deployment.yml.j2`.
Deployment files stored at :code:`conf/deployment.(json|yml|yaml).j2`. will be auto-discovered.

Please find examples on how to use Jinja2 templates below:

.. tabs::

   .. tab:: deployment.json.j2

      .. literalinclude:: ../../tests/deployment-configs/jinja-example.json.j2
         :language: jinja

   .. tab:: deployment.yml.j2

      .. literalinclude:: ../../tests/deployment-configs/jinja-example.yaml.j2
         :language: yaml+jinja
