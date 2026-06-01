# MedCAT LLM Validator

The `LLMAnnotationValidator` is a clinical NLP auditor designed to validate the accuracy of MedCAT extractions. It treats MedCAT's output as a hypothesis and leverages Large Language Models (LLMs) to adjudicate whether the extracted metadata (negation, temporality, experiencer, etc.) matches the actual clinical context.

## Architecture Overview

The validator operates as a modular, task-based system. It sits between raw data extraction and downstream vectorization, allowing you to filter out false positives or increase confidence in specific clinical concepts.

### Core Components

*   **`LLMAnnotationValidator`**: The main orchestrator that manages context fetching, task execution loops, and final status derivation.
*   **`LLMInteractionService`**: Manages communication with the LLM, handling connectivity retries (3 attempts) and JSON parsing retries (5 attempts).
*   **`Task Manager`**: Dynamically loads validation logic from JSON files in the `tasks/` directory.
*   **`Context Manager`**: Retrieves and prepares clinical text snippets (windows) surrounding an annotation to provide sufficient context to the LLM.
*   **`DebugLogManager`**: Provides deep traceability by recording every prompt, raw response, and logical reflection.

## How It Works

For every annotation processed, the validator:
1.  **Fetches Context**: Retrieves the original document text using a `TextFetcher`.
2.  **Identifies Tasks**: Determines which tasks (e.g., Negation, Experiencer) are enabled.
3.  **Executes LLM Calls**: Routes specific prompts for each task to the LLM.
4.  **Adjudicates**: Compares the LLM's response to the original MedCAT prediction.
5.  **Derives Status**: Assigns an overall `llm_status` based on the combined results of all tasks.

## Understanding Adjudication Statuses

The `llm_status` represents the final verdict of the audit:

| Status | Meaning |
| :--- | :--- |
| **Confirmed** | The LLM agreed with MedCAT on all checked attributes. |
| **Rejected** | The LLM disagreed with MedCAT on at least one attribute (e.g., MedCAT said "Present" but LLM saw "Negated"). |
| **Uncertain** | The LLM response could not be parsed into a valid JSON block after retries. |
| **Skipped** | The concept was not in the `concepts_to_validate` list. |
| **Error** | An issue occurred (e.g., source text could not be found in the database). |

## Task Configuration (JSON)

Tasks are defined in JSON files located in `pat2vec/util/llm/tasks/`. This makes the system extensible without changing the core Python code.

**Example Task (`negated.json`):**
```json
{
    "task_id": "negated",
    "task_key": "NEGATED",
    "status_role": "negation_gate",
    "reference_column": "Presence_Value",
    "potential_outputs": "Affirmed, Negated, Hypothetical",
    "output_column": "llm_negated"
}
```

## Usage Example

You can use the high-level `validate_annotations` helper to process a DataFrame.

```python
from pat2vec.util.llm import validate_annotations

# Define your LLM caller (e.g., a LangChain model or a simple function)
def my_llm_caller(prompt):
    return llm.invoke(prompt)

# Run validation
results_df = validate_annotations(
    df_or_path=my_annotations_df,
    config_obj=config,
    llm_caller=my_llm_caller,
    concepts_to_validate=["C0011847", "Diabetes"],
    debug_mode=True
)
```

## Debugging & Reflections

One of the most powerful features of the validator is the **Reflection** system. When `debug_mode` is enabled, the validator doesn't just store a status; it stores the LLM's reasoning for every decision.

### In Jupyter Notebooks:

You can use the `debug_display` utilities to analyze performance:

```python
from pat2vec.util.llm.debug_display import show_reflections, inspect_row_interactions

# Show a table of all disagreements between LLM and Human Ground Truth
show_reflections(validator)

# Deep dive into the prompt/response for a specific row
inspect_row_interactions(validator, row_idx=42)
```

## Benchmarking (MedCAT Trainer)

To evaluate the LLM itself against human labels:
1.  Export your data from MedCAT Trainer.
2.  Convert it using `medcat_json_to_validation_dataset()`.
3.  Run the validator.
4.  The system will automatically generate **Reflections** explaining why the LLM disagreed with the human label.

## Prompting Best Practices

*   **Analysis First**: Prompts are designed to ask the LLM for a "Brief Analysis" before the JSON block. This forces "Chain of Thought" processing, leading to higher accuracy.
*   **Concise Mode**: For faster models, use `use_concise_prompts=True` to strip away conversational filler.
*   **Context Window**: Use `max_chars` to control how much surrounding text is sent. Standard clinical notes often need ~3000 characters to capture relevant history.

## Canonical Terminology

To ensure consistency, the validator normalizes all inputs and outputs to these terms:
*   **Negation**: `Affirmed`, `Negated`, `Hypothetical`.
*   **Temporality**: `Present`, `Past`, `Future`.
*   **Experiencer**: `Patient`, `Relative`, `Other`.
