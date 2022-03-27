import dunamai as _dunamai

__version__ = _dunamai.get_version(
    "dbx", third_choice=_dunamai.Version.from_git
).serialize()
