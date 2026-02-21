from __future__ import annotations

import unittest

from ontario_job_bot.sheets import (
    _normalize_header,
    _worksheet_matches_orgs,
    _worksheet_supports_org_updates,
)


class SheetsOrgSyncTests(unittest.TestCase):
    def test_normalize_header_aliases(self) -> None:
        self.assertEqual(_normalize_header("Organization Name"), "org_name")
        self.assertEqual(_normalize_header("Website URL"), "homepage_url")
        self.assertEqual(_normalize_header("Job URL"), "jobs_url")

    def test_worksheet_matches_required_columns(self) -> None:
        headers = ["Org ID", "Org Name", "Org Type", "Homepage URL", "Jobs URL"]
        self.assertTrue(_worksheet_matches_orgs(headers))

    def test_worksheet_rejects_missing_required_columns(self) -> None:
        headers = ["Org ID", "Org Name", "Org Type", "Homepage URL"]
        self.assertFalse(_worksheet_matches_orgs(headers))

    def test_worksheet_supports_delta_updates(self) -> None:
        headers = ["Organization", "Jobs/Careers URL"]
        self.assertTrue(_worksheet_supports_org_updates(headers))


if __name__ == "__main__":
    unittest.main()
