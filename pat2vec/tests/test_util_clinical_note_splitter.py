import unittest
import pandas as pd
from pandas import Timestamp
from pat2vec.util.clinical_note_splitter import (
    find_date,
    split_clinical_notes,
    split_and_append_chunks,
)


class TestClinicalNoteSplitter(unittest.TestCase):
    def test_find_date_basic(self):
        """Test extracting date-stamped chunks from text."""
        txt = "Entered on - 26-Oct-2023 12:00 Part A. Entered on - 27-Oct-2023 14:00 Part B."
        chunks = find_date(txt)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(
            chunks[0]["date"], pd.to_datetime("2023-10-26 12:00", utc=True)
        )
        self.assertTrue(any("Part A" in c["text"] for c in chunks))

    def test_find_date_no_timestamp_fallback(self):
        """Test behavior when regex matches but no date is found in the window."""
        txt = "Entered on - [No Date] Content here."
        fallback = Timestamp("2023-01-01")
        chunks = find_date(txt, original_update_time_value=fallback)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["date"], fallback)
        self.assertFalse(chunks[0]["date_found"])

    def test_find_date_malformed_string(self):
        """Test that garbage strings in the date window don't crash the parser."""
        txt = "Entered on - ThisIsNotADate and some text."
        fallback = Timestamp("2023-10-26")
        chunks = find_date(txt, original_update_time_value=fallback)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["date"], fallback)
        self.assertFalse(chunks[0]["date_found"])
        self.assertTrue(len(chunks[0]["text"]) > 0)

    def test_split_clinical_notes_epr(self):
        """Test splitting EPR-formatted clinical notes into a dataframe."""
        df = pd.DataFrame(
            {
                "id": [1],
                "client_idcode": ["P1"],
                "body_analysed": [
                    "Entered on - 26-Oct-2023 10:00 Text 1. Entered on - 27-Oct-2023 11:00 Text 2."
                ],
                "updatetime": [pd.to_datetime("2023-01-01", utc=True)],
                "document_description": ["Clinical Note"],
                "document_guid": ["G1"],
                "clientvisit_visitidcode": ["V1"],
                "_index": ["idx1"],
            }
        )
        processed, none_rows = split_clinical_notes(df)
        self.assertEqual(len(processed), 2)
        self.assertEqual(
            processed.iloc[1]["updatetime"],
            pd.to_datetime("2023-10-27 11:00", utc=True),
        )
        self.assertTrue(none_rows.empty)

    def test_split_and_append_chunks_orchestration(self):
        """Verify filtering and concatenation logic in the splitter wrapper."""
        df = pd.DataFrame(
            {
                "_id": ["M1", "M2"],
                "client_idcode": ["P1", "P1"],
                "observation_valuetext_analysed": [
                    "Entered on - 26-Oct-2023 12:00 MCT Text.",
                    "Static Note",
                ],
                "observationdocument_recordeddtm": [
                    pd.to_datetime("2023-10-25"),
                    pd.to_datetime("2023-10-25"),
                ],
                "obscatalogmasteritem_displayname": [
                    "AoMRC_ClinicalSummary_FT",
                    "Blood Pressure",
                ],
                "clientvisit_visitidcode": ["V1", "V1"],
                "observation_guid": ["G1", "G2"],
                "_index": ["idx1", "idx1"],
            }
        )
        # Act with mct=True
        result = split_and_append_chunks(df, epr=False, mct=True)
        # Result should contain the split chunk for the first row and the original second row
        self.assertEqual(len(result), 2)
        self.assertIn(
            "AoMRC_ClinicalSummary_FT_clinical note chunk_0",
            result["obscatalogmasteritem_displayname"].values,
        )
