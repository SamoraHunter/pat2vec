import os
import matplotlib.pyplot as plt


def generate_pie_charts(all_batch_annots, save_plots=True, types=None):
    """
    Generate pie charts for each unique client_id in the given DataFrame.

    Parameters:
    - all_batch_annots (pd.DataFrame): The DataFrame containing annotation data.
    - save_plots (bool, optional): If True, save the generated pie charts as PNG files.
                                   If False, only display the charts without saving. Default is True.
    - types (list, optional): List of types to create additional pie charts for.
                              Default is ["['procedure']", "['disorder']", "['finding']"].
    """

    # Create a folder for saving the plots
    output_folder = "plot_outputs_folder_piechart"
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
