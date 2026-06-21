import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import os
import tempfile
import shutil

# Import functions to be tested
from pat2vec.util.evaluation_methods_ploting import (
    plot_roc_curve,
    plot_precision_recall_curve,
    plot_calibration_curve,
    plot_feature_importance,
    plot_confusion_matrix,
    plot_missing_data_patterns,
)


class TestEvaluationMethodsPloting(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.output_path = os.path.join(self.test_dir, "test_plot.png")
        self.mock_config = MagicMock()
        self.mock_config.root_path = self.test_dir
        self.mock_config.proj_name = "test_project"
        self.mock_config.verbosity = 0

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    def test_plot_roc_curve_basic(self, mock_plt):
        """Test basic ROC curve plotting."""
        y_true = np.array([0, 0, 1, 1])
        y_pred = np.array([0.1, 0.4, 0.35, 0.8])
        model_name = "TestModel"

        plot_roc_curve(y_true, y_pred, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_plt.savefig.assert_called_once()
        mock_plt.show.assert_called_once()

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_plot_roc_curve_empty_data(self, mock_show, mock_savefig):
        """Test ROC curve plotting with empty data."""
        y_true = np.array([])
        y_pred = np.array([])
        model_name = "TestModel"

        plot_roc_curve(y_true, y_pred, model_name, self.mock_config)

        mock_savefig.assert_not_called()
        mock_show.assert_not_called()
        self.assertFalse(os.path.exists(self.output_path))

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    def test_plot_precision_recall_curve_basic(self, mock_plt):
        """Test basic Precision-Recall curve plotting."""
        y_true = np.array([0, 0, 1, 1])
        y_pred = np.array([0.1, 0.4, 0.35, 0.8])
        model_name = "TestModel"

        plot_precision_recall_curve(y_true, y_pred, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_plt.savefig.assert_called_once()

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    def test_plot_calibration_curve_basic(self, mock_plt):
        """Test basic calibration curve plotting."""
        y_true = np.array([0, 0, 1, 1])
        y_pred = np.array([0.1, 0.4, 0.35, 0.8])
        model_name = "TestModel"

        plot_calibration_curve(y_true, y_pred, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_plt.savefig.assert_called_once()

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    def test_plot_feature_importance_basic(self, mock_plt):
        """Test basic feature importance plotting."""
        feature_importances = pd.DataFrame(
            {"importance": [0.5, 0.3, 0.2]}, index=["feat1", "feat2", "feat3"]
        )
        model_name = "TestModel"

        plot_feature_importance(feature_importances, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_plt.savefig.assert_called_once()

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    @patch("matplotlib.pyplot.figure")
    def test_plot_feature_importance_empty_data(
        self, mock_figure, mock_show, mock_savefig
    ):
        """Test feature importance plotting with empty data."""
        feature_importances = pd.DataFrame({"importance": []})
        model_name = "TestModel"

        plot_feature_importance(feature_importances, model_name, self.mock_config)

        mock_savefig.assert_not_called()
        mock_show.assert_not_called()
        self.assertFalse(os.path.exists(self.output_path))

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    @patch("pat2vec.util.evaluation_methods_ploting.sns")
    def test_plot_confusion_matrix_basic(self, mock_sns, mock_plt):
        """Test basic confusion matrix plotting."""
        y_true = np.array([0, 0, 1, 1])
        y_pred = np.array([0, 1, 0, 1])
        model_name = "TestModel"

        plot_confusion_matrix(y_true, y_pred, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_sns.heatmap.assert_called_once()
        mock_plt.savefig.assert_called_once()

    @patch("pat2vec.util.evaluation_methods_ploting.plt")
    @patch("pat2vec.util.evaluation_methods_ploting.sns")
    def test_plot_missing_data_patterns_basic(self, mock_sns, mock_plt):
        """Test basic missing data patterns plotting."""
        df = pd.DataFrame({"col1": [1, 2, np.nan], "col2": [4, np.nan, 6]})
        model_name = "TestModel"

        plot_missing_data_patterns(df, model_name, self.mock_config)

        mock_plt.figure.assert_called()
        mock_sns.heatmap.assert_called_once()
        mock_plt.savefig.assert_called_once()

    @patch("matplotlib.pyplot.savefig")
    @patch("matplotlib.pyplot.show")
    def test_plot_missing_data_patterns_empty_df(self, mock_show, mock_savefig):
        """Test missing data patterns plotting with an empty DataFrame."""
        df = pd.DataFrame()
        model_name = "TestModel"

        plot_missing_data_patterns(df, model_name, self.mock_config)

        mock_savefig.assert_not_called()
        mock_show.assert_not_called()
        self.assertFalse(os.path.exists(self.output_path))


if __name__ == "__main__":
    unittest.main()
