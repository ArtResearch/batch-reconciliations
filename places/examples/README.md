Examples are based on the data from Fondazione Federico Zeri. They demonstrate a chained, hierarchical reconciliation process, starting from countries and progressively moving down to regions, districts, and cities.

### Step 1: Reconcile Countries

This first step takes a simple list of country names and finds their canonical TGN URIs.

**Command:**
```bash
python3 ../reconcile_countries.py ./countries.csv 2 > ./reconciled_countries.csv 
```

**Explanation of Parameters:**
*   `../reconcile_countries.py`: The script used for simple, non-hierarchical reconciliation.
*   `./countries.csv`: The input file. For example:
    ```csv
    count,PRVS[@etichetta='Country']
    70003,Italia
    19055,Stati Uniti d'America
    15595,Regno Unito
    ```
*   `2`: This specifies that the country names (e.g., "Italia") are located in the **2nd column** of `countries.csv`.
*   `> ./reconciled_countries.csv`: This redirects the script's output into a new file.

After running, manual review of the output may be necessary to correct ambiguous matches (like for "Russia") or fill in missing ones ("Stati Uniti d'America"). The corrected file, `reconciled_countries_corrected.csv`, serves as our top-level context for the next step.

### Step 2: Reconcile Regions

This step uses the corrected countries file as a "context" to reconcile a list of regions (e.g., states, provinces) within those countries.

**Command:**
```bash
python3 ../reconcile_region.py \
    --regions-input-file ./regions.csv \
    --top-region-def-file ./reconciled_countries_corrected.csv \
    --ri-top-region-name-col 2 \
    --ri-region-name-col 3 \
    --trd-name-col 2 \
    --trd-uri-col 7 \
    > ./reconciled_regions.csv
```

**Explanation of Parameters:**
*   `--regions-input-file ./regions.csv`: The input file with regions to reconcile. For example:
    ```csv
    count,PRVS[@etichetta='Country'],PRVR[@etichetta='Region / Federal State']
    16474,Italia,Lazio
    16336,Italia,Toscana
    9801,Stati Uniti d'America,New York
    ```
*   `--top-region-def-file ./reconciled_countries_corrected.csv`: Our **context** file, which contains the reconciled country URIs.
*   `--ri-top-region-name-col 2`: In `regions.csv`, the country name (e.g., "Italia") is in the **2nd column**. This is the context lookup key.
*   `--ri-region-name-col 3`: In `regions.csv`, the region name to reconcile (e.g., "Lazio") is in the **3rd column**.
*   `--trd-name-col 2`: In the `reconciled_countries_corrected.csv` definition file, the country name to match against is in the **2nd column**.
*   `--trd-uri-col 7`: In that same definition file, the corresponding TGN URI for the country is in the **7th column**.

### Step 3: Reconcile Districts

This step demonstrates using a multi-part context (Country + Region) to reconcile districts.

**Command:**
```bash
python3 ../reconcile_region.py \
    --regions-input-file ./districts.csv \
    --top-region-def-file ./reconciled_regions.csv \
    --ri-top-region-name-col "2,3" \
    --ri-region-name-col 4 \
    --trd-name-col "2,3" \
    --trd-uri-col 12 \
    > ./reconciled_districts.csv
```

**Explanation of Parameters:**
*   `--regions-input-file ./districts.csv`: The input file. For example:
    ```csv
    count,PVCS[@etichetta='Country'],PVCR[@etichetta='Region / Federal State'],PVCP[@etichetta='District']
    15453,Italia,Lazio,Roma
    10738,Italia,Toscana,Firenze
    ```
*   `--ri-top-region-name-col "2,3"`: The context in `districts.csv` is composed of two columns: the country ("Italia", **column 2**) and the region ("Lazio", **column 3**).
*   `--ri-region-name-col 4`: The district to reconcile ("Roma") is in the **4th column**.
*   `--trd-name-col "2,3"`: In the `reconciled_regions.csv` definition file, the context to match is also the country and region in columns **2 and 3**.
*   `--trd-uri-col 12`: The TGN URI for the matching *region* is in the **12th column** of `reconciled_regions.csv`.

### Step 4: Reconcile Cities (Multi-Context Fallback)

This final example uses multiple definition files to provide layered contexts. The script attempts to match using the most specific context first (districts), falling back to regions, and then to countries if no match is found.

**Command:**
```bash
python3 ../reconcile_region.py \
    --remove-trailing-state \
    --regions-input-file ./cities.csv \
    --ri-top-region-name-col "2,3,4" \
    --ri-region-name-col 5 \
    --top-region-def-file ./reconciled_districts.csv --trd-name-col "2,3,4" --trd-uri-col 13 \
    --top-region-def-file ./reconciled_regions.csv --trd-name-col "2,3" --trd-uri-col 12 \
    --top-region-def-file ./reconciled_countries_corrected.csv --trd-name-col 2 --trd-uri-col 7 \
    > ./reconciled_cities.csv
```

**Explanation of Parameters:**
*   `--remove-trailing-state`: A flag to clean city names like "New York (NY)" to "New York" before querying.
*   `--regions-input-file ./cities.csv`: The input file. For example:
    ```csv
    count,PRVS,PRVR,PRVP,PRVC
    14943,Italia,Lazio,Roma,Roma
    9600,Stati Uniti d'America,New York,,New York (NY)
    ```
*   `--ri-top-region-name-col "2,3,4"`: The context in `cities.csv` is Country (**col 2**), Region (**col 3**), and District (**col 4**).
*   `--ri-region-name-col 5`: The city name to reconcile (e.g., "Roma") is in the **5th column**.
*   `--top-region-def-file ...`: This argument is specified three times to create a fallback chain:
    1.  **Try `reconciled_districts.csv` first:** Match using Country+Region+District (cols "2,3,4") to get the district's URI (col 13).
    2.  **If that fails, try `reconciled_regions.csv`:** Match using just Country+Region (cols "2,3") to get the region's URI (col 12).
    3.  **If that fails, try `reconciled_countries_corrected.csv`:** Match using just the Country (col 2) to get its URI (col 7).
