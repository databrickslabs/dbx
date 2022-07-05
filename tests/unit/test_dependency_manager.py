from dbx.utils.dependency_manager import DependencyManager


def test_not_matching_conditions(tmp_path, capsys):
    dm = DependencyManager(no_rebuild=True, global_no_package=True)

    reference = {"deployment_config": {"no_package": False}}

    dm.process_dependencies(reference)
    captured = capsys.readouterr()
    assert "--no-package option is set to true" in captured.out
