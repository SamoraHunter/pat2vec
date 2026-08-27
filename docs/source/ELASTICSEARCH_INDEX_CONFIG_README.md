# Elasticsearch Index Configuration

This directory contains the centralized configuration for mapping pat2vec get methods to their source Elasticsearch indices.

## Files

- **`elasticsearch_index_config.py`** - Python module with programmatic access to index configurations
- **`ELASTICSEARCH_INDEX_CONFIG.json`** - JSON export (auto-generated, ignored by git)

## Usage

### Programmatic Access

```python
from pat2vec.util.elasticsearch_index_config import (
    get_all_index_configs,
    get_index_config_for_method,
    print_index_config_summary,
)

# Get all configurations
configs = get_all_index_configs()

# Get config for a specific method
config = get_index_config_for_method("get_current_pat_bloods")
print(f"Index: {config.index_pattern}")
print(f"Time Field: {config.time_field}")

# Print human-readable summary
print_index_config_summary()
```

### Manual Review

1. Run `python pat2vec/util/elasticsearch_index_config.py` to see a summary in terminal
2. Or generate `ELASTICSEARCH_INDEX_CONFIG.json` for detailed review:

```bash
python -c "
import json
from pat2vec.util.elasticsearch_index_config import get_all_index_configs
configs = get_all_index_configs()
data = {
    'metadata': {
        'generated_at': 'auto-generated',
        'total_methods': len(configs)
    },
    'configs': {name: config.to_dict() for name, config in sorted(configs.items())}
}
with open('ELASTICSEARCH_INDEX_CONFIG.json', 'w') as f:
    json.dump(data, f, indent=2)
"
```

## Configuration Structure

Each method configuration includes:

- **`index_pattern`**: Elasticsearch index pattern (e.g., `pims_apps*`, `observations`)
- **`fields`**: List of fields queried by the get method
- **`time_field`**: Primary time field used for filtering (e.g., `document_UpdatedWhen`, `AppointmentDateTime`)
- **`date_fields`**: Additional date/time fields that may be present
- **`description`**: Human-readable description

## Common Time Fields by Index

### PIMS Indices
- `pims_apps*`: `AppointmentDateTime`, `DateModified`

### Observations Indices
- `observations`: Various (check specific method config)
  - Bed: no time field
  - BMI: `observationdocument_recordeddtm`
  - Core02: no time field

### Basic Observations
- `basic_observations`:
  - Bloods: `basicobs_entered`
  - COVID: no time field

### Order Indices (Drugs/Diagnostics)
- `order`: `order_createdwhen`

### Epic Indices
Check method-specific configs for date fields.

## Troubleshooting

If you encounter mismatches between source cluster and expected configuration:

1. **Verify index exists**: Check the actual Elasticsearch index names match patterns in config
2. **Check time field existence**: Ensure the configured time_field actually exists in the index mapping
3. **Review fields**: Confirm all expected fields are present in the source index
4. **Cross-reference**: Compare `ELASTICSEARCH_INDEX_CONFIG.json` with your cluster's actual mappings

## Adding New Methods

1. Define index pattern (e.g., `"my_new_index*"` or exact name)
2. Import field constants from the get method module
3. Specify time_field if filtering by date is needed
4. Add to `GET_ALL_INDEX_CONFIGS()` function with description
