from __future__ import annotations

from .adp import AdpAdapter
from .generic import GenericAdapter
from .html_list import HtmlListAdapter
from .icims import IcimsAdapter
from .neogov import NeogovAdapter
from .pdf import PdfAdapter
from .taleo import TaleoAdapter
from .utipro import UtiproAdapter
from .workday import WorkdayAdapter


def get_adapter(adapter_name: str):
    key = (adapter_name or "").strip().lower()
    mapping = {
        "workday": WorkdayAdapter(),
        "taleo": TaleoAdapter(),
        "icims": IcimsAdapter(),
        "neogov": NeogovAdapter(),
        "utipro": UtiproAdapter(),
        "adp": AdpAdapter(),
        "html_list": HtmlListAdapter(),
        "pdf": PdfAdapter(),
        "generic": GenericAdapter(),
    }
    return mapping.get(key, GenericAdapter())
