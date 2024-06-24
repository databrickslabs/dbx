from typing import Any

from dbx.api.adjuster.mixins.base import ElementSetterMixin
from dbx.utils.file_uploader import AbstractFileUploader


class FileReferenceAdjuster(ElementSetterMixin):
    def __init__(self, file_uploader: AbstractFileUploader):
        self._uploader = file_uploader

    def adjust_file_ref(self, element: str, parent: Any, index: Any):
        _uploaded = self._uploader.upload_and_provide_path(element)
        self.set_element_at_parent(_uploaded, parent, index)
