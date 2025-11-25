# GLIF Pipeline — Dynamic & Optimized

## Setup

1. **Install dependencies:**

   ```bash
   pip install pyspark
   ```

2. **Download JDBC jars:**

   - `ojdbc8.jar` → place in `c:\Users\ABHI\Docer_Poject\`

3. **Create config:**

   - Edit `config.json` with your HDFS/Oracle details

4. **Prepare logs directory:**
   ```bash
   mkdir logs
   ```

## Usage

### Run with default (today's date):

```bash
python new_GLIF_optimized.py --config ./config.json
```

### Run for specific date:

```bash
python new_GLIF_optimized.py --config ./config.json --date 2025-11-20
```

### Import as module:

```python
from new_GLIF_optimized import run_glif_pipeline
from datetime import date

run_glif_pipeline("./config.json", processing_date=date(2025, 11, 20))
```

## Key Improvements

- **Dynamic Configuration:** All parameters in `config.json` (no code changes needed)
- **Parameterized Dates:** Process any date via CLI or API
- **Optimizations:**
  - Broadcast join for CGL validation
  - Reduced aggregations (single pass)
  - Repartitioning before writes
  - Arrow execution enabled
  - Adaptive query optimization
- **Logging:** File + console logging with row counts, timings, error traces
- **Error Handling:** Graceful degradation, retry-friendly structure
- **Reusability:** Functions extracted, callable as module or CLI
- **Performance Tuning:** Configurable partition counts, batch sizes, memory

## Configuration Schema

See `config.json` for all tuneable parameters:

- Spark configs (partitions, memory, adaptive settings)
- Oracle connection details
- HDFS paths and file patterns
- Processing constants (substring positions, scale factors)
- Table names
- Logging level and file path
