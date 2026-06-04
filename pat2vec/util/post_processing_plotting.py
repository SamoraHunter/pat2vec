import matplotlib.pyplot as plt
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def plot_missing_pattern_bloods(dfb: pd.DataFrame) -> None:
    """Plots missing client_idcodes for the top 50 most frequent items."""
    top_items = dfb["basicobs_itemname_analysed"].value_counts().nlargest(50).index
    filtered_dfb = dfb[dfb["basicobs_itemname_analysed"].isin(top_items)]

    clients_per_item = filtered_dfb.groupby("basicobs_itemname_analysed")[
        "client_idcode"
    ].apply(set)
    all_clients = set(dfb["client_idcode"])

    missing_clients_counts = {
        item: len(all_clients - clients) for item, clients in clients_per_item.items()
    }

    missing_clients_df = pd.DataFrame(
        list(missing_clients_counts.items()),
        columns=["basicobs_itemname_analysed", "missing_client_count"],
    ).sort_values(by="missing_client_count", ascending=False)

    plt.figure(figsize=(10, 8))
    plt.barh(
        missing_clients_df["basicobs_itemname_analysed"],
        missing_clients_df["missing_client_count"],
        color="skyblue",
    )
    plt.xlabel("Number of Missing Client ID Codes")
    plt.ylabel("Basicobs Item Name Analysed")
    plt.title("Missing Client ID Codes per Top 50 Basicobs Item Names")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.show()
