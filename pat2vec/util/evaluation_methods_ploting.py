import os
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from typing import List, Optional
from sklearn.metrics import (
    roc_curve,
    auc,
    precision_recall_curve,
    average_precision_score,
    confusion_matrix,
)
from sklearn.calibration import calibration_curve


def plot_roc_curve(y_true, y_score, model_name, config) -> None:
    """Plots the Receiver Operating Characteristic (ROC) curve."""
    if len(y_true) == 0:
        return
    fpr, tpr, _ = roc_curve(y_true, y_score)
    roc_auc = auc(fpr, tpr)
    plt.figure()
    plt.plot(fpr, tpr, label=f"ROC curve (area = {roc_auc:0.2f})")
    plt.plot([0, 1], [0, 1], "k--")
    plt.title(f"ROC - {model_name}")
    plt.legend(loc="lower right")
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def plot_precision_recall_curve(y_true, y_score, model_name, config) -> None:
    """Plots the Precision-Recall curve."""
    if len(y_true) == 0:
        return
    precision, recall, _ = precision_recall_curve(y_true, y_score)
    avg_precision = average_precision_score(y_true, y_score)
    plt.figure()
    plt.plot(recall, precision, label=f"AP = {avg_precision:0.2f}")
    plt.title(f"PR Curve - {model_name}")
    plt.legend(loc="lower left")
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def plot_calibration_curve(y_true, y_prob, model_name, config) -> None:
    """Plots the calibration curve (reliability diagram)."""
    if len(y_true) == 0:
        return
    prob_true, prob_pred = calibration_curve(y_true, y_prob, n_bins=10)
    plt.figure()
    plt.plot(prob_pred, prob_true, "s-", label=model_name)
    plt.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")
    plt.title(f"Calibration - {model_name}")
    plt.legend()
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def plot_feature_importance(importances, model_name, config) -> None:
    """Plots feature importances as a horizontal bar chart."""
    if importances.empty:
        return
    plt.figure(figsize=(10, 6))
    importances.sort_values().plot(kind="barh")
    plt.title(f"Feature Importance - {model_name}")
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def plot_confusion_matrix(y_true, y_pred, model_name, config) -> None:
    """Plots a confusion matrix heatmap."""
    if len(y_true) == 0:
        return
    cm = confusion_matrix(y_true, y_pred)
    plt.figure()
    sns.heatmap(cm, annot=True, fmt="d")
    plt.title(f"Confusion Matrix - {model_name}")
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def plot_missing_data_patterns(df, model_name, config) -> None:
    """Plots a heatmap of missing data patterns."""
    if df.empty:
        return
    plt.figure(figsize=(12, 6))
    sns.heatmap(df.isnull(), cbar=False)
    plt.title(f"Missing Data Patterns - {model_name}")
    plt.savefig(os.path.join(config.root_path, "test_plot.png"))
    plt.show()


def generate_pie_charts(
    all_batch_annots: pd.DataFrame,
    save_plots: bool = True,
    types: Optional[List[str]] = None,
    output_folder: str = "plot_outputs_folder_piechart",
) -> None:
    """Generates and saves pie charts of annotation distributions for each client.

    For each unique `client_idcode` in the input DataFrame, this function
    creates pie charts summarizing the distribution of `pretty_name` for
    annotations. It generates one chart for all annotation types combined and
    separate charts for each type specified in the `types` list.

    To improve readability, concepts in the bottom 25th percentile by count
    are grouped into an "other" category.

    Args:
        all_batch_annots: DataFrame containing annotation data with columns
            like 'client_idcode', 'pretty_name', and 'types'.
        save_plots: If True, saves the charts as PNG files in a local
            'plot_outputs_folder_piechart' directory.
        output_folder: Directory to save plots in. Defaults to 'plot_outputs_folder_piechart'.
        types: A list of annotation types (e.g., "['disorder']") to generate
            separate pie charts for. Defaults to a predefined list of common types.
    """
    # Create a folder for saving the plots
    os.makedirs(output_folder, exist_ok=True)

    # Assuming all_batch_annots is your DataFrame
    unique_clients = all_batch_annots["client_idcode"].unique()

    # Set default types if not provided
    if types is None:
        types = ["['procedure']", "['disorder']", "['finding']"]

    for client_id in unique_clients:
        # Filter dataframe for the specific client_idcode
        client_data = all_batch_annots[all_batch_annots["client_idcode"] == client_id]

        # Create a pie chart for pretty_name column
        cui_counts = client_data["pretty_name"].value_counts()

        # Identify the bottom 25% of values
        bottom_25_threshold = cui_counts.quantile(0.25)
        bottom_25_values = cui_counts[cui_counts <= bottom_25_threshold].index

        # Group the bottom 25% into "other"
        cui_counts.loc[cui_counts.index.isin(bottom_25_values)] = cui_counts[
            ~cui_counts.index.isin(bottom_25_values)
        ].sum()
        cui_counts = cui_counts[~cui_counts.index.isin(bottom_25_values)]
        cui_counts["other"] = cui_counts[cui_counts.index.isin(bottom_25_values)].sum()

        # Create a larger figure with 1080p resolution
        plt.figure(figsize=(16, 9), dpi=100)

        # Plot the pie chart without a legend
        cui_counts.plot(kind="pie", autopct="%1.1f%%", startangle=90)
        plt.title(f"Pie Chart for Client ID: {client_id} - All Types")
        plt.axis(
            "equal"
        )  # Equal aspect ratio ensures that the pie is drawn as a circle.

        # Save the plot if specified
        if save_plots:
            output_filename_all_types = os.path.join(
                output_folder, f"pie_chart_all_types_client_{client_id}.png"
            )
            plt.savefig(output_filename_all_types, bbox_inches="tight")

        plt.close()

        # Create additional plots for specified types
        for ctype in types:
            type_data = client_data[client_data["types"] == ctype]
            type_counts = type_data["pretty_name"].value_counts()

            # Identify the bottom 25% of values for each type
            type_bottom_25_threshold = type_counts.quantile(0.25)
            type_bottom_25_values = type_counts[
                type_counts <= type_bottom_25_threshold
            ].index

            # Group the bottom 25% into "other" for each type
            type_counts.loc[type_counts.index.isin(type_bottom_25_values)] = (
                type_counts[~type_counts.index.isin(type_bottom_25_values)].sum()
            )
            type_counts = type_counts[~type_counts.index.isin(type_bottom_25_values)]
            type_counts["other"] = type_counts[
                type_counts.index.isin(type_bottom_25_values)
            ].sum()

            # Create a larger figure with 1080p resolution for each type
            plt.figure(figsize=(16, 9), dpi=100)

            # Plot the pie chart without a legend for each type
            type_counts.plot(kind="pie", autopct="%1.1f%%", startangle=90)
            plt.title(f"Pie Chart for Client ID: {client_id} - Type: {ctype}")
            plt.axis(
                "equal"
            )  # Equal aspect ratio ensures that the pie is drawn as a circle.

            # Save the plot if specified
            if save_plots:
                output_filename_type = os.path.join(
                    output_folder, f"pie_chart_type_{ctype}_client_{client_id}.png"
                )
                plt.savefig(output_filename_type, bbox_inches="tight")

            plt.close()
