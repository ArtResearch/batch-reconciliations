import argparse
import csv
import json
import requests
import sys
from collections import defaultdict

SPARQL_ENDPOINT_URL = "https://dev.artresearch.net/sparql?repository=3rd-party"
SPARQL_USERNAME = ""
SPARQL_PASSWORD = ""

# SPARQL query for TGN
SPARQL_QUERY_TEMPLATE = """
PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>
PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX getty: <http://vocab.getty.edu/ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?term (SAMPLE(?wikidata_label_coalesced) AS ?wikidata_label) (SAMPLE(?label_en_coalesced) AS ?label_en) (SAMPLE(?label_it_coalesced) AS ?label_it) (SAMPLE(?label_de_coalesced) AS ?label_de) (SAMPLE(?label_fr_coalesced) AS ?label_fr) (SAMPLE(?scope_note_x) AS ?scope_note) (SAMPLE(?wikidata_description_coalesced) AS ?wikidata_description) (SAMPLE(?wikidata_uri_coalesced) AS ?wikidata_uri) {{
  # values_clause and ?i removed for direct search term injection

    ?term skosxl:prefLabel|skosxl:altLabel ?entity .
    ?term getty:placeTypePreferred/getty:broaderPreferred* <http://vocab.getty.edu/aat/300232420> . # Sovereign State
    ?entity getty:term ?found_label_uri .
    FILTER(REGEX(?found_label_uri, "^{search_word_direct}$", "i")) .

    # English Label (Pref or Alt)
    OPTIONAL {{
      ?term skosxl:prefLabel ?enPrefLabelEntity .
      ?enPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enPrefLabelEntity getty:term ?pref_label_en .
    }}
    OPTIONAL {{
      ?term skosxl:altLabel ?enAltLabelEntity .
      ?enAltLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enAltLabelEntity getty:term ?alt_label_en .
    }}
    BIND(COALESCE(?pref_label_en, ?alt_label_en) AS ?label_en_coalesced) .

    # Italian Label (Pref or Alt)
    OPTIONAL {{
      ?term skosxl:prefLabel ?itPrefLabelEntity .
      ?itPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itPrefLabelEntity getty:term ?pref_label_it .
    }}
    OPTIONAL {{
      ?term skosxl:altLabel ?itAltLabelEntity .
      ?itAltLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itAltLabelEntity getty:term ?alt_label_it .
    }}
    BIND(COALESCE(?pref_label_it, ?alt_label_it) AS ?label_it_coalesced) .

    # German Label (Pref or Alt)
    OPTIONAL {{
      ?term skosxl:prefLabel ?dePrefLabelEntity .
      ?dePrefLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?dePrefLabelEntity getty:term ?pref_label_de .
    }}
    OPTIONAL {{
      ?term skosxl:altLabel ?deAltLabelEntity .
      ?deAltLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?deAltLabelEntity getty:term ?alt_label_de .
    }}
    BIND(COALESCE(?pref_label_de, ?alt_label_de) AS ?label_de_coalesced) .

    # French Label (Pref or Alt)
    OPTIONAL {{
      ?term skosxl:prefLabel ?frPrefLabelEntity .
      ?frPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frPrefLabelEntity getty:term ?pref_label_fr .
    }}
    OPTIONAL {{
      ?term skosxl:altLabel ?frAltLabelEntity .
      ?frAltLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frAltLabelEntity getty:term ?alt_label_fr .
    }}
    BIND(COALESCE(?pref_label_fr, ?alt_label_fr) AS ?label_fr_coalesced) .
    
    # Scope Note
    OPTIONAL {{
      ?term <http://www.w3.org/2004/02/skos/core#scopeNote>/rdf:value ?scope_note_x .
    }}

    # Wikidata Integration
    OPTIONAL {{
      ?term dc:identifier ?tgn_id_str .
      SERVICE <https://qlever.cs.uni-freiburg.de/api/wikidata> {{
        ?wd_uri_raw wdt:P1667 ?tgn_id_str . # P1667 is TGN ID
        OPTIONAL {{
          ?wd_uri_raw rdfs:label ?wd_label_raw .
          FILTER (lang(?wd_label_raw) = "en") .
        }}
        OPTIONAL {{
          ?wd_uri_raw schema:description ?wd_desc_raw .
          FILTER (lang(?wd_desc_raw) = "en") .
        }}
      }}
    }}
    BIND(COALESCE(?wd_uri_raw, "") AS ?wikidata_uri_coalesced)
    BIND(COALESCE(?wd_label_raw, "") AS ?wikidata_label_coalesced)
    BIND(COALESCE(?wd_desc_raw, "") AS ?wikidata_description_coalesced)
}} GROUP BY ?term
"""

def parse_arguments():
    parser = argparse.ArgumentParser(description="Reconcile country names from a CSV column against the TGN SPARQL endpoint.")
    parser.add_argument("csv_filename", help="Path to the input CSV file.")
    parser.add_argument("column_number", type=int, help="1-indexed column number containing text to reconcile.")
    return parser.parse_args()

def read_csv_data(filename, column_idx):
    """Reads CSV data and extracts values from the specified column."""
    original_rows = []
    texts_to_query = {} 
    
    with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        original_rows.append(header) 

        for i, row in enumerate(reader):
            original_rows.append(row)
            if len(row) > column_idx:
                text = row[column_idx]
                if text and text not in texts_to_query: # Ensure text is not empty
                    texts_to_query[text] = i
            else:
                print(f"Warning: Row {i+1} is too short for column {column_idx + 1}. Skipping text extraction for this row.", file=sys.stderr)
                
    sparql_values = [(text, texts_to_query[text]) for text in texts_to_query]
    return header, original_rows[1:], sparql_values

# build_sparql_values_clause is removed as queries are now made one by one.

def execute_sparql_query(query):
    """Executes the SPARQL query and returns the JSON response."""
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth = (SPARQL_USERNAME, SPARQL_PASSWORD.replace("&", "&")) # Use actual '&' for auth
    try:
        response = requests.post(SPARQL_ENDPOINT_URL, data={"query": query}, headers=headers, auth=auth, timeout=300)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error executing SPARQL query: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status code: {e.response.status_code}", file=sys.stderr)
            print(f"Response text: {e.response.text}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding SPARQL JSON response: {e}", file=sys.stderr)
        print(f"Response content: {response.text}", file=sys.stderr)
        return None

# process_results is removed; its logic is integrated into the main loop.

def write_output_csv(original_header, original_data_rows, processed_sparql_results):
    """Writes the final CSV to stdout."""
    writer = csv.writer(sys.stdout)
    
    new_header = original_header + [
        "number_of_results", 
        "wikidata_label", "label_en", "label_it", "label_de", "label_fr", 
        "scope_note", "wikidata_description", 
        "term", "wikidata_uri"
    ]
    writer.writerow(new_header)

    for i, original_row_data in enumerate(original_data_rows):
        sparql_matches = processed_sparql_results.get(i, [])
        num_results = len(sparql_matches)

        if num_results == 0:
            # num_results, wikidata_label, label_en, label_it, label_de, label_fr, scope_note, wikidata_description, term, wikidata_uri
            writer.writerow(original_row_data + [0, "", "", "", "", "", "", "", "", ""]) 
        else:
            # If multiple matches for one input, write each on a new row but only list num_results once for the first.
            for match_idx, match in enumerate(sparql_matches):
                current_num_results = num_results if match_idx == 0 else "" # Show count only for the first line of a multi-match
                writer.writerow(original_row_data + [
                    current_num_results,
                    match.get("wikidata_label", ""),
                    match.get("label_en", ""),
                    match.get("label_it", ""),
                    match.get("label_de", ""),
                    match.get("label_fr", ""),
                    match.get("scope_note", ""),
                    match.get("wikidata_description", ""),
                    match.get("term", ""), 
                    match.get("wikidata_uri", "")
                ])

def main():
    args = parse_arguments()
    column_idx_0_based = args.column_number - 1

    if column_idx_0_based < 0:
        print("Error: Column number must be 1 or greater.", file=sys.stderr)
        sys.exit(1)

    original_header, original_data_rows, texts_with_indices_for_sparql = read_csv_data(args.csv_filename, column_idx_0_based)
    
    if not texts_with_indices_for_sparql:
        print("No text found in the specified column to query.", file=sys.stderr)
        writer = csv.writer(sys.stdout)
        new_header = original_header + [
            "number_of_results", 
            "wikidata_label", "label_en", "label_it", "label_de", "label_fr", 
            "scope_note", "wikidata_description", 
            "term", "wikidata_uri"
        ]
        writer.writerow(new_header)
        for row_data in original_data_rows:
            # num_results, wikidata_label, label_en, label_it, label_de, label_fr, scope_note, wikidata_description, term, wikidata_uri
            writer.writerow(row_data + [0, "", "", "", "", "", "", "", "", ""]) 
        sys.exit(0)

    processed_sparql_data = defaultdict(list)
    total_queries_to_make = len(texts_with_indices_for_sparql)
    
    if total_queries_to_make > 0:
        print(f"Starting SPARQL queries for {total_queries_to_make} country terms...", file=sys.stderr)

    for idx, (text, original_row_idx) in enumerate(texts_with_indices_for_sparql):
        # Escape backslashes and double quotes for SPARQL string literal
        # Also escape single quotes if they are part of the text, as the search term will be enclosed in double quotes in the query.
        # However, ql:contains-word typically handles this well. The primary concern is breaking the SPARQL query structure.
        # For `ql:contains-word "search_term"`, only double quotes within search_term need escaping.
        escaped_text = text.replace('\\', '\\\\').replace('"', '\\"')
        
        # The search term is directly injected into the query.
        query = SPARQL_QUERY_TEMPLATE.format(search_word_direct=escaped_text)

        # print(f"DEBUG: Query {idx+1}/{total_queries_to_make} for '{text}':\n{query}", file=sys.stderr) # Uncomment for debugging
        print(f"Executing query {idx+1}/{total_queries_to_make} for term: '{text}' (original row index: {original_row_idx})", file=sys.stderr)
        
        sparql_response_json = execute_sparql_query(query)
        
        if sparql_response_json and "results" in sparql_response_json and "bindings" in sparql_response_json["results"]:
            bindings = sparql_response_json["results"]["bindings"]
            if not bindings:
                print(f"Info: No match found for term: '{text}' (original row index: {original_row_idx})", file=sys.stderr)
            
            for binding in bindings:
                try:
                    # original_row_idx is known from the Python loop context.
                    # No ?i is expected in the binding anymore.
                    result_item = {
                        "wikidata_label": binding.get("wikidata_label", {}).get("value", ""),
                        "label_en": binding.get("label_en", {}).get("value", ""),
                        "label_it": binding.get("label_it", {}).get("value", ""),
                        "label_de": binding.get("label_de", {}).get("value", ""),
                        "label_fr": binding.get("label_fr", {}).get("value", ""),
                        "scope_note": binding.get("scope_note", {}).get("value", ""),
                        "wikidata_description": binding.get("wikidata_description", {}).get("value", ""),
                        "term": binding.get("term", {}).get("value", ""),
                        "wikidata_uri": binding.get("wikidata_uri", {}).get("value", "")
                    }
                    # Ensure term is present, as it's key
                    if not result_item["term"]:
                        print(f"Warning: Query for '{text}' (original row index: {original_row_idx}) succeeded but ?term is missing in result. Binding: {binding}", file=sys.stderr)
                    # Allow appending even if term is missing; write_output_csv will handle empty strings.
                    processed_sparql_data[original_row_idx].append(result_item)
                except (KeyError, ValueError) as e:
                    print(f"Warning: Could not process a SPARQL binding for term '{text}' (original row index: {original_row_idx}): {binding}. Error: {e}", file=sys.stderr)
                    continue
        else:
            # execute_sparql_query already prints errors for network/request issues.
            # This handles cases where the response might be non-JSON or missing expected structure.
            print(f"Warning: Query failed or returned malformed/empty data for term: '{text}' (original row index: {original_row_idx})", file=sys.stderr)

    if total_queries_to_make > 0:
        print(f"Finished SPARQL queries for {total_queries_to_make} country terms.", file=sys.stderr)
    
    write_output_csv(original_header, original_data_rows, processed_sparql_data)

if __name__ == "__main__":
    main()
