# Place Name Reconciliation Scripts

This directory contains Python scripts designed to reconcile geographical place names against authoritative knowledge bases, specifically the Getty Thesaurus of Geographic Names (TGN) and Wikidata. The process, often called "reconciliation," involves matching text strings (like "Italy" or "Rome") to unique, stable identifiers (URIs) from these databases.

The scripts are designed to be used in a chained, hierarchical workflow, starting with large geographical entities like countries and progressively reconciling smaller, contained places like regions, districts, and cities.

## Scripts

### `reconcile_countries.py`

This script performs a straightforward reconciliation for a list of countries.

**Purpose:** To find canonical TGN and Wikidata URIs for sovereign states provided in a CSV file.

**Conceptual Workflow:**
1.  **Input:** Takes a CSV file and a column number containing country names.
2.  **Querying:** For each unique country name, it constructs and executes a SPARQL query against a TGN endpoint. The query specifically looks for entities classified as "Sovereign States" (`aat:300232420`).
3.  **Data Retrieval:** It fetches rich data for each match, including:
    *   The canonical TGN URI (`term`).
    *   Labels in multiple languages (English, Italian, German, French).
    *   A descriptive `scope_note`.
    *   The corresponding Wikidata URI and description.
4.  **Output:** It streams a new CSV to standard output. This CSV contains all the original data from the input file, augmented with new columns for the reconciled data.

### `reconcile_region.py`

This is a more advanced script for hierarchical reconciliation. It finds places that exist *within* another, larger place.

**Purpose:** To reconcile places like states, provinces, districts, or cities by using a known parent entity (e.g., a country or a state) as a context to disambiguate the name.

**Conceptual Workflow:**
1.  **Input:** The script requires two main types of input:
    *   An input file containing the places to be reconciled (e.g., a list of cities).
    *   One or more "top-region definition files." These are previously reconciled CSVs (like the output of `reconcile_countries.py`) that map the names of parent regions to their TGN URIs.
2.  **Contextual Search:** For each place in the input file, the script first determines its context. For example, to reconcile "Florence," it looks up the TGN URI for its parent, "Italy," from a definition file. It then executes a SPARQL query that searches for "Florence" *only within the hierarchy* of Italy's TGN entry. This contextual constraint is extremely powerful for finding the correct entity and avoiding ambiguity (e.g., Florence, Italy vs. Florence, South Carolina).
3.  **Sophisticated Ranking:** The search query is highly optimized. It ranks potential matches based on:
    *   **Place Type:** It prioritizes more logical matches (e.g., a political division is ranked higher than a general inhabited place).
    *   **Hierarchical Distance:** It prefers matches that are closer to the parent entity in the TGN's `broaderPreferred` hierarchy.
4.  **Multi-level Fallback System:** If the primary contextual TGN search fails, the script automatically initiates a series of fallbacks to find a match:
    *   **Wikidata-to-TGN Fallback:** It queries Wikidata for an entity with the same name that is geographically located within the parent entity. If this Wikidata entity has a TGN identifier, the script uses it to fetch the full data from TGN.
    *   **Wikidata-Only Fallback:** If the first fallback fails, it performs a similar search on Wikidata but will accept a match even if it lacks a TGN identifier, using just the Wikidata URI as the result.
5.  **Global Search:** If a place cannot be matched using any provided context (or if no context is available), the script falls back one last time to a "global" search. This search queries TGN and Wikidata for the place name without any hierarchical constraints.
6.  **Output:** Like the countries script, it streams an enriched CSV to standard output, merging the best-found match with the original data. The output CSV is designed to be seamlessly used as a definition file for the next level of reconciliation (e.g., using reconciled regions to find districts).
