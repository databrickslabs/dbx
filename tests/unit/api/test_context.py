from dbx.api.context import LocalContextManager
from dbx.models.context import ContextInfo


def test_local_context_serde():
    ctx_id = "aaa111aaabbb"
    LocalContextManager.set_context(ContextInfo(context_id=ctx_id))
    result = LocalContextManager.get_context()
    assert ctx_id == result.context_id


def test_local_context_non_existent():
    LocalContextManager.context_file_path.unlink(missing_ok=True)
    result = LocalContextManager.get_context()
    assert result is None
