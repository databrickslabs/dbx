Jinja2 Support: Environment variables, logic and loops
======================================================

Basic template support
----------------------


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

Support for includes
--------------------

Jinja2-based templates also support :code:`include` clause which allows you to re-share common bits of configuration across multiple files and improve modularity of configurations.

For example, your main deployment file can look like this:

.. literalinclude:: ../../tests/deployment-configs/nested-configs/09-jinja-include.json.j2
         :language: jinja

And in the :code:`includes` folder you can provide the cluster configuration component:

.. literalinclude:: ../../tests/deployment-configs/nested-configs/includes/cluster-test.json.j2
         :language: jinja


Environment variables
---------------------

Since version 0.6.0  :code:`dbx` supports passing environment variables into the deployment configuration, giving you an additional level of flexibility.
You can pass environment variables both into JSON and YAML-based configurations which are written in Jinja2 template format.
This allows you to parametrize the deployment and make it more flexible for CI pipelines.

.. tabs::

   .. tab:: JSON

      .. literalinclude:: ../../tests/deployment-configs/04-jinja-with-env-vars.json.j2
         :language: jinja

   .. tab:: YAML

      .. literalinclude:: ../../tests/deployment-configs/04-jinja-with-env-vars.yaml.j2
         :language: yaml+jinja
