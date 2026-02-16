from __future__ import annotations

import unittest

from ontario_job_bot.title_normalize_and_validate import (
    extract_title_hierarchy_from_detail,
    validate_title_and_job_gate,
)


class TitleNormalizeValidateTests(unittest.TestCase):
    def test_submit_service_rejected(self) -> None:
        result = validate_title_and_job_gate(
            candidate_title="Submit a Service",
            posting_url="https://example.ca/services/submit-a-service",
            source_url="https://example.ca/services",
            title_source="anchor",
            listing_text="",
            detail_text="",
            has_jsonld_jobposting=False,
            listing_signal=False,
        )
        self.assertFalse(result.accepted)
        self.assertEqual(result.rejection_type, "blocklist")

    def test_notices_page_rejected(self) -> None:
        result = validate_title_and_job_gate(
            candidate_title="Notices",
            posting_url="https://example.ca/notices/public-notice-123",
            source_url="https://example.ca/notices",
            title_source="h1",
            listing_text="",
            detail_text="",
            has_jsonld_jobposting=False,
            listing_signal=False,
        )
        self.assertFalse(result.accepted)
        self.assertEqual(result.rejection_type, "blocklist")

    def test_real_job_title_accepted_and_cleaned(self) -> None:
        result = validate_title_and_job_gate(
            candidate_title="RECRUITMENT - 20260130 - Environmental Program Coordinator Job Description 2026",
            posting_url="https://example.ca/employment/environmental-program-coordinator",
            source_url="https://example.ca/careers",
            title_source="anchor",
            listing_text="Apply now. Closing date March 30, 2026. Salary range listed.",
            detail_text="Department: Public Works",
            has_jsonld_jobposting=False,
            listing_signal=True,
        )
        self.assertTrue(result.accepted)
        self.assertEqual(result.normalized_title, "Environmental Program Coordinator")

    def test_hierarchy_prefers_jsonld_over_heading(self) -> None:
        html = """
        <html>
          <head>
            <script type=\"application/ld+json\">
              {
                \"@context\": \"https://schema.org\",
                \"@type\": \"JobPosting\",
                \"title\": \"Building Inspector II\"
              }
            </script>
            <meta property=\"og:title\" content=\"Careers - Town of Example\" />
          </head>
          <body>
            <main>
              <h1>View Posting</h1>
            </main>
          </body>
        </html>
        """
        resolved = extract_title_hierarchy_from_detail(
            detail_html=html,
            page_url="https://example.ca/jobs/123",
            fallback_anchor_title="View Posting",
        )
        self.assertEqual(resolved.title, "Building Inspector II")
        self.assertEqual(resolved.title_source, "jsonld")


if __name__ == "__main__":
    unittest.main()
