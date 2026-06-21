"""Tests for pat2vec/util/evaluation_methods_ploting.py."""

import os
import tempfile
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from pat2vec.util.evaluation_methods_ploting import (
    plot_roc_curve,
    plot_precision_recall_curve,
    plot_calibration_curve,
    plot_feature_importance,
    plot_confusion_matrix,
    plot_missing_data_patterns,
    generate_pie_charts,
)


@pytest.fixture
def mock_config():
    """Create a mock config object with root_path."""
    config = MagicMock()
    config.root_path = tempfile.mkdtemp()
    return config


class TestPlotROCCurve:
    """Tests for plot_roc_curve function."""

    def test_plot_roc_curve_normal_data(self, mock_config):
        """Test ROC curve plotting with normal data."""
        y_true = [0, 0, 1, 1]
        y_score = [0.1, 0.4, 0.35, 0.8]

        # Should not raise an error
        plot_roc_curve(y_true, y_score, "test_model", mock_config)

        # Verify file was created
        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_roc_curve_empty_data(self, mock_config):
        """Test ROC curve with empty data - should return early."""
        y_true = []
        y_score = []

        result = plot_roc_curve(y_true, y_score, "test_model", mock_config)
        assert result is None

    def test_plot_roc_curve_single_class(self, mock_config):
        """Test ROC curve with single class - edge case."""
        y_true = [0, 0, 0, 0]
        y_score = [0.1, 0.4, 0.35, 0.8]

        plot_roc_curve(y_true, y_score, "test_model", mock_config)


class TestPlotPrecisionRecallCurve:
    """Tests for plot_precision_recall_curve function."""

    def test_plot_precision_recall_normal_data(self, mock_config):
        """Test PR curve plotting with normal data."""
        y_true = [0, 0, 1, 1]
        y_score = [0.1, 0.4, 0.35, 0.8]

        plot_precision_recall_curve(y_true, y_score, "test_model", mock_config)

        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_precision_recall_empty_data(self, mock_config):
        """Test PR curve with empty data - should return early."""
        y_true = []
        y_score = []

        result = plot_precision_recall_curve(y_true, y_score, "test_model", mock_config)
        assert result is None

    def test_plot_precision_recall_all_positive(self, mock_config):
        """Test PR curve with all positive labels."""
        y_true = [1, 1, 1, 1]
        y_score = [0.5, 0.6, 0.7, 0.8]

        plot_precision_recall_curve(y_true, y_score, "test_model", mock_config)


class TestPlotCalibrationCurve:
    """Tests for plot_calibration_curve function."""

    def test_plot_calibration_normal_data(self, mock_config):
        """Test calibration curve plotting with normal data."""
        y_true = [0, 0, 1, 1]
        y_prob = [0.2, 0.4, 0.6, 0.8]

        plot_calibration_curve(y_true, y_prob, "test_model", mock_config)

        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_calibration_empty_data(self, mock_config):
        """Test calibration curve with empty data - should return early."""
        y_true = []
        y_prob = []

        result = plot_calibration_curve(y_true, y_prob, "test_model", mock_config)
        assert result is None

    def test_plot_calibration_perfectly_calibrated(self, mock_config):
        """Test calibration curve with perfectly calibrated predictions."""
        y_true = [0, 0, 1, 1]
        y_prob = [0.25, 0.25, 0.75, 0.75]

        plot_calibration_curve(y_true, y_prob, "test_model", mock_config)


class TestPlotFeatureImportance:
    """Tests for plot_feature_importance function."""

    def test_plot_feature_importance_normal_data(self, mock_config):
        """Test feature importance plotting with normal data."""
        importances = pd.DataFrame(
            {"feature": ["A", "B", "C"], "importance": [0.1, 0.3, 0.6]}
        )

        plot_feature_importance(importances, "test_model", mock_config)

        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_feature_importance_empty_dataframe(self, mock_config):
        """Test feature importance with empty DataFrame - should return early."""
        importances = pd.DataFrame()

        result = plot_feature_importance(importances, "test_model", mock_config)
        assert result is None

    def test_plot_feature_importance_single_feature(self, mock_config):
        """Test feature importance with single feature."""
        importances = pd.DataFrame({"feature": ["A"], "importance": [1.0]})

        plot_feature_importance(importances, "test_model", mock_config)


class TestPlotConfusionMatrix:
    """Tests for plot_confusion_matrix function."""

    def test_plot_confusion_matrix_normal_data(self, mock_config):
        """Test confusion matrix plotting with normal data."""
        y_true = [0, 1, 1, 0]
        y_pred = [0, 1, 0, 0]

        plot_confusion_matrix(y_true, y_pred, "test_model", mock_config)

        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_confusion_matrix_empty_data(self, mock_config):
        """Test confusion matrix with empty data - should return early."""
        y_true = []
        y_pred = []

        result = plot_confusion_matrix(y_true, y_pred, "test_model", mock_config)
        assert result is None

    def test_plot_confusion_matrix_perfect_classification(self, mock_config):
        """Test confusion matrix with perfect classification."""
        y_true = [0, 0, 1, 1]
        y_pred = [0, 0, 1, 1]

        plot_confusion_matrix(y_true, y_pred, "test_model", mock_config)

    def test_plot_confusion_matrix_all_wrong(self, mock_config):
        """Test confusion matrix where all predictions are wrong."""
        y_true = [0, 0, 1, 1]
        y_pred = [1, 1, 0, 0]

        plot_confusion_matrix(y_true, y_pred, "test_model", mock_config)


class TestPlotMissingDataPatterns:
    """Tests for plot_missing_data_patterns function."""

    def test_plot_missing_data_normal_df(self, mock_config):
        """Test missing data patterns plotting with normal DataFrame."""
        df = pd.DataFrame(
            {"A": [1, 2, None, 4], "B": [None, 2, 3, None], "C": [1, None, None, 4]}
        )

        plot_missing_data_patterns(df, "test_model", mock_config)

        expected_file = os.path.join(mock_config.root_path, "test_plot.png")
        assert os.path.exists(expected_file)

    def test_plot_missing_data_empty_df(self, mock_config):
        """Test missing data patterns with empty DataFrame - should return early."""
        df = pd.DataFrame()

        result = plot_missing_data_patterns(df, "test_model", mock_config)
        assert result is None

    def test_plot_missing_data_no_missing_values(self, mock_config):
        """Test missing data patterns with no missing values."""
        df = pd.DataFrame({"A": [1, 2, 3, 4], "B": [5, 6, 7, 8]})

        plot_missing_data_patterns(df, "test_model", mock_config)

    def test_plot_missing_data_all_missing(self, mock_config):
        """Test missing data patterns with all values missing."""
        df = pd.DataFrame({"A": [None, None, None], "B": [None, None, None]})

        plot_missing_data_patterns(df, "test_model", mock_config)


class TestGeneratePieCharts:
    """Tests for generate_pie_charts function."""

    def test_generate_pie_charts_multiple_clients(self, mock_config):
        """Test pie chart generation with multiple clients."""
        all_batch_annots = pd.DataFrame(
            {
                "client_idcode": [" patient1", "patient2", "patient1", "patient2"],
                "pretty_name": [
                    "Condition A",
                    "Condition B",
                    "Condition C",
                    "Condition A",
                ],
                "types": [
                    "['disorder']",
                    "['procedure']",
                    "['finding']",
                    "['disorder']",
                ],
            }
        )

        output_folder = tempfile.mkdtemp()

        # Mock plt.show and plt.savefig to avoid GUI issues
        with patch("pat2vec.util.evaluation_methods_ploting.plt") as mock_plt:
            generate_pie_charts(
                all_batch_annots,
                save_plots=False,
                types=["['disorder']"],
                output_folder=output_folder,
            )

            # Verify plt methods were called
            assert mock_plt.figure.called
            assert mock_plt.title.called

    def test_generate_pie_charts_single_client(self, mock_config):
        """Test pie chart generation with single client."""
        all_batch_annots = pd.DataFrame(
            {
                "client_idcode": ["patient1", "patient1"],
                "pretty_name": ["Condition A", "Condition B"],
                "types": ["['disorder']", "['procedure']"],
            }
        )

        output_folder = tempfile.mkdtemp()

        with patch("pat2vec.util.evaluation_methods_ploting.plt"):
            generate_pie_charts(
                all_batch_annots,
                save_plots=False,
                types=["['disorder']"],
                output_folder=output_folder,
            )

    def test_generate_pie_charts_empty_dataframe(self, mock_config):
        """Test pie chart generation with empty DataFrame."""
        all_batch_annots = pd.DataFrame(
            {"client_idcode": [], "pretty_name": [], "types": []}
        )

        output_folder = tempfile.mkdtemp()

        # Should not raise an error
        result = generate_pie_charts(
            all_batch_annots,
            save_plots=False,
            types=["['disorder']"],
            output_folder=output_folder,
        )
        assert result is None

    def test_generate_pie_charts_default_types(self, mock_config):
        """Test pie chart generation with default types."""
        all_batch_annots = pd.DataFrame(
            {
                "client_idcode": ["patient1", "patient2"],
                "pretty_name": ["Condition A", "Condition B"],
                "types": ["['procedure']", "['disorder']"],
            }
        )

        output_folder = tempfile.mkdtemp()

        with patch("pat2vec.util.evaluation_methods_ploting.plt"):
            generate_pie_charts(
                all_batch_annots, save_plots=False, output_folder=output_folder
            )

    def test_generate_pie_charts_multiple_types(self, mock_config):
        """Test pie chart generation with multiple custom types."""
        all_batch_annots = pd.DataFrame(
            {
                "client_idcode": ["patient1"] * 4,
                "pretty_name": ["A", "B", "C", "D"],
                "types": [
                    "['procedure']",
                    "['disorder']",
                    "['finding']",
                    "['substance']",
                ],
            }
        )

        output_folder = tempfile.mkdtemp()

        with patch("pat2vec.util.evaluation_methods_ploting.plt"):
            generate_pie_charts(
                all_batch_annots,
                save_plots=False,
                types=["['procedure']", "['disorder']"],
                output_folder=output_folder,
            )

    def test_generate_pie_charts_with_save(self, mock_config):
        """Test pie chart generation with saving to files."""
        all_batch_annots = pd.DataFrame(
            {
                "client_idcode": ["patient1"],
                "pretty_name": ["Condition A"],
                "types": ["['disorder']"],
            }
        )

        output_folder = tempfile.mkdtemp()

        # With save_plots=True and temp directory
        with patch("pat2vec.util.evaluation_methods_ploting.plt"):
            generate_pie_charts(
                all_batch_annots,
                save_plots=True,
                types=["['disorder']"],
                output_folder=output_folder,
            )
