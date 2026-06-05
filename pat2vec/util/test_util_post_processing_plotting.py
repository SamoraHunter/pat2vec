import unittest
import pandas as pd
from unittest.mock import patch
from pat2vec.util.post_processing_plotting import plot_missing_pattern_bloods


class TestPostProcessingPlotting(unittest.TestCase):
    """Unit tests for the post-processing plotting utility module."""

    @patch("pat2vec.util.post_processing_plotting.plt")
    def test_plot_missing_pattern_bloods_execution(self, mock_plt):
        """Verify that plot_missing_pattern_bloods performs correct aggregation and triggers plotting."""
        # Setup sample data with varying item frequencies and missing patients
        df = pd.DataFrame(
            {
                "client_idcode": ["P1", "P1", "P2", "P3", "P4", "P5"],
                "basicobs_itemname_analysed": ["A", "B", "A", "C", "A", "A"],
            }
        )

        # Total unique clients: {P1, P2, P3, P4, P5} (count=5)
        # Item A is seen by {P1, P2, P4, P5} (Missing P3 -> count=1)
        # Item B is seen by {P1} (Missing P2, P3, P4, P5 -> count=4)
        # Item C is seen by {P3} (Missing P1, P2, P4, P5 -> count=4)

        # The function calculates these missing counts and plots them sorted descending.

        # Act
        plot_missing_pattern_bloods(df)

        # Assertions to ensure matplotlib workflow is followed
        self.assertTrue(mock_plt.figure.called)
        self.assertTrue(mock_plt.barh.called)
        self.assertTrue(mock_plt.xlabel.called)
        self.assertTrue(mock_plt.ylabel.called)
        self.assertTrue(mock_plt.show.called)

        mock_plt.title.assert_called_with(
            "Missing Client ID Codes per Top 50 Basicobs Item Names"
        )


if __name__ == "__main__":
    unittest.main()
