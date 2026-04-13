# Guide to `example_searches_test.ipynb`

This guide will walk you through using the `example_searches_test.ipynb` notebook to explore and test the search capabilities of `pat2vec`. This notebook is designed to help you understand how to build cohorts and retrieve data from Elasticsearch using various search methods.

## 1. Prerequisites

Before you begin, ensure you have completed the following:

- **Installation**: `pat2vec` is fully installed by following the Installation Guide.
- **Environment Activation**: The `pat2vec_env` virtual environment is activated.
- **Configuration**: Your `credentials.py` file is populated with Elasticsearch credentials, and your MedCAT model is in the `medcat_models` directory as described in the Usage Guide.

## 2. Opening the Notebook

1.  Navigate to the `notebooks` directory inside your `pat2vec` repository clone:
    ```shell
    cd pat2vec/notebooks/
    ```
2.  Start Jupyter Lab or Jupyter Notebook:
    ```shell
    jupyter lab
    ```
3.  Open the `example_searches_test.ipynb` file from the Jupyter interface.

## 3. Selecting the Kernel

Once the notebook is open, make sure you are using the correct Python environment.

- In the top-right corner of the notebook, click on the kernel name.
- Select `pat2vec_env` from the list. If it's not there, you may need to restart Jupyter after activating the virtual environment.

## 4. Configuring the Search

The notebook uses a `config_class` object to manage all search parameters. This is where you will define what you want to search for.

### Key Configuration Steps:

1.  **Import `config_class`**: The first few cells will handle necessary imports from `pat2vec.util.config_pat2vec`.
2.  **Instantiate `config_class`**: You will find a cell where `config_class` is instantiated.
    ```python
    from pat2vec.util.config_pat2vec import config_class

    # Instantiate the configuration
    config = config_class(
        # ... parameters will be here
    )
    ```
3.  **Set `testing` mode**: For this example notebook, you will likely run in `testing=True` mode. This uses dummy data generators and does not require a live connection to a real patient data index. When you are ready to work with real data, you will set this to `False`.
    ```python
    config = config_class(
        testing=True,
        # ... other parameters
    )
    ```
4.  **Define Search Parameters**: The notebook will demonstrate how to use different cohort searchers. You will configure parameters such as:
    - `index_name`: The Elasticsearch index to search against.
    - `search_terms`: Keywords or concepts to find.
    - `method`: The search strategy to use (`"fuzzy"`, `"exact"`, or `"phrase"`).
    - `fuzzy` and `slop`: Fine-grained controls for matching logic.
    - `date_columns`: Fields to use for temporal filtering.
    - `start_year`, `start_month`, etc.: Defining the time window for extraction.

For a complete list of all possible configuration options, refer to the Comprehensive Configuration Guide.

## 5. Running the Searches

The notebook is divided into sections, each demonstrating a different type of search.

- **Execute cells sequentially**: Run the cells one by one by pressing `Shift + Enter`.
- **Observe the output**: Each search will produce a pandas DataFrame containing the patient IDs and other data that match the criteria. The notebook will display the head of this DataFrame.

The examples will likely cover:
- **Simple Term Searches**: Finding patients based on the presence of specific keywords in their records.
- **Temporal Searches**: Filtering cohorts based on dates.
- **Combined Searches**: Using multiple criteria to build a more specific cohort.

## 6. Understanding the Results

The primary output of each search is a **cohort DataFrame**. This DataFrame typically contains at least a `client_idcode` column, which you can then use as input for the main `pat2vec` feature extraction pipeline as shown in `example_usage.ipynb`.

You can also use the `check_patients_existence` function to verify which IDs from an external list actually exist across your Elasticsearch indices.

By experimenting with `example_searches_test.ipynb`, you can become proficient at defining and extracting the precise patient cohorts you need for your research.
