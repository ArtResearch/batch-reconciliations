import argparse
import csv
import json
import re # Added for regex operations
import requests
import sys
from collections import defaultdict

# TGN SPARQL Endpoint and Credentials
SPARQL_ENDPOINT_URL = "https://dev.artresearch.net/sparql?repository=3rd-party"
SPARQL_USERNAME = ""
SPARQL_PASSWORD = ""

# Wikidata SPARQL Endpoint
WIKIDATA_SPARQL_ENDPOINT_URL = "https://qlever.cs.uni-freiburg.de/api/wikidata"

# SPARQL query for TGN regions, based on reconcile_region.py logic
SINGLE_REGION_TGN_SPARQL_QUERY_TEMPLATE = """
PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX getty: <http://vocab.getty.edu/ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX gvp: <http://vocab.getty.edu/ontology#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>

SELECT ?tgn_uri (SAMPLE(?label_en_coalesced) AS ?label_en) (SAMPLE(?label_it_coalesced) AS ?label_it) (SAMPLE(?label_de_coalesced) AS ?label_de) (SAMPLE(?label_fr_coalesced) AS ?label_fr) (SAMPLE(?type_term) AS ?type) (SAMPLE(?scope_note_x) AS ?scope_note) (SAMPLE(?label_gvp_term) AS ?label) (SAMPLE(?wikidata_uri_coalesced) AS ?wikidata_uri) (SAMPLE(?wikidata_description_coalesced) AS ?wikidata_description) WHERE {{
  # Subquery to find candidate entities and calculate their priority rank
  {{
    SELECT ?tgn_uri (MIN(?distance_rank_val) AS ?min_distance_rank) (MIN(?type_rank_val) AS ?final_type_rank)
    WHERE {{
        # Label matching
        ?tgn_uri skosxl:prefLabel|skosxl:altLabel ?entity .
        ?entity getty:term ?found_label_uri .
        FILTER(REGEX(?found_label_uri, "^{search_term_direct}$", "i")) .

        # Path length constraints relative to the top_region_uri (distance_rank)
        # ?tgn_uri must be within N levels of top_region_uri using getty:broaderPreferred
        {{ ?tgn_uri getty:broaderPreferred <{top_region_uri}> . BIND(1 AS ?distance_rank_val) }}
        UNION
        {{ ?tgn_uri getty:broaderPreferred/getty:broaderPreferred <{top_region_uri}> . BIND(2 AS ?distance_rank_val) }}
        UNION
        {{ ?tgn_uri getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred <{top_region_uri}> . BIND(3 AS ?distance_rank_val) }}
        UNION
        {{ ?tgn_uri getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred <{top_region_uri}> . BIND(4 AS ?distance_rank_val) }}
        UNION
        {{ ?tgn_uri getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred/getty:broaderPreferred <{top_region_uri}> . BIND(5 AS ?distance_rank_val) }}

        # Place Type Ranking for places
        # Rank 1: Preferred place type is a political divison (or narrower)
        # Rank 2: Preferred place type is an inhabited place (or narrower)
        # Rank 3: Non-Preferred place type is inhabited place (or narrower)
        # Rank 4: Other or not specified as inhabited place
        OPTIONAL {{
            ?tgn_uri (getty:placeTypePreferred)/(getty:broaderPreferred*) <http://vocab.getty.edu/aat/300236157> .
            BIND(1 AS ?type_pref_match)
        }}
        OPTIONAL {{
            ?tgn_uri (getty:placeTypePreferred)/(getty:broaderPreferred*) <http://vocab.getty.edu/aat/300008347> .
            BIND(2 AS ?type_pref_match)
        }}
        OPTIONAL {{
            # TODO, need to prioritize inhabitet places for cities (comment retained from original)
            ?tgn_uri (getty:placeTypeNonPreferred)/(getty:broaderPreferred*) <http://vocab.getty.edu/aat/300008347> .
            BIND(3 AS ?type_nonpref_match)
        }}
        #FILTER(BOUND(?type_pref_match) || BOUND(?type_nonpref_match)) . # Removed to allow rank 3 for non-matches
        BIND(COALESCE(?type_pref_match, ?type_nonpref_match, 4) AS ?type_rank_val) # Assign 3 if no specific type match

    }} GROUP BY ?tgn_uri
  }}

  # Fetch details for the ranked ?tgn_uri(s) from the TGN graph
    # English Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?enPrefLabelEntity .
      ?enPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enPrefLabelEntity getty:term ?pref_label_en .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?enAltLabelEntity .
      ?enAltLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enAltLabelEntity getty:term ?alt_label_en .
    }}
    BIND(COALESCE(?pref_label_en, ?alt_label_en) AS ?label_en_coalesced) .

    # Italian Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?itPrefLabelEntity .
      ?itPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itPrefLabelEntity getty:term ?pref_label_it .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?itAltLabelEntity .
      ?itAltLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itAltLabelEntity getty:term ?alt_label_it .
    }}
    BIND(COALESCE(?pref_label_it, ?alt_label_it) AS ?label_it_coalesced) .

    # German Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?dePrefLabelEntity .
      ?dePrefLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?dePrefLabelEntity getty:term ?pref_label_de .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?deAltLabelEntity .
      ?deAltLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?deAltLabelEntity getty:term ?alt_label_de .
    }}
    BIND(COALESCE(?pref_label_de, ?alt_label_de) AS ?label_de_coalesced) .

    # French Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?frPrefLabelEntity .
      ?frPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frPrefLabelEntity getty:term ?pref_label_fr .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?frAltLabelEntity .
      ?frAltLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frAltLabelEntity getty:term ?alt_label_fr .
    }}
    BIND(COALESCE(?pref_label_fr, ?alt_label_fr) AS ?label_fr_coalesced) .

    # Getty Place Type (Preferred GVP Term)
    OPTIONAL {{
      ?tgn_uri getty:placeTypePreferred ?placeTypeEntity .
      ?placeTypeEntity getty:prefLabelGVP ?prefGVPLabelEntity .
      ?prefGVPLabelEntity getty:term ?type_term .
    }}
    
    OPTIONAL {{
      ?tgn_uri <http://www.w3.org/2004/02/skos/core#scopeNote>/rdf:value ?scope_note_x .
    }}

    # GVP Label (prefLabelGVP/term)
    OPTIONAL {{
      ?tgn_uri gvp:prefLabelGVP ?gvpLabelEntity .
      ?gvpLabelEntity gvp:term ?label_gvp_term .
      # Assuming gvp:prefLabelGVP does not have explicit language tags in the same way skosxl:prefLabel does.
      # If language filtering is needed for gvp:term, it would require a different structure or assumptions.
    }}

    # Get TGN ID for Wikidata lookup
    ?tgn_uri dc:identifier ?tgn_id_str .
    OPTIONAL {{
    # Wikidata Service Call
      SERVICE <https://qlever.cs.uni-freiburg.de/api/wikidata> {{
        ?wd_uri wdt:P1667 ?tgn_id_str .
        OPTIONAL {{
          ?wd_uri schema:description ?wd_desc .
          FILTER (lang(?wd_desc) = "en") .
        }}
      }}
   }}
    BIND(COALESCE(?wd_uri, "") AS ?wikidata_uri_coalesced)
    BIND(COALESCE(?wd_desc, "") AS ?wikidata_description_coalesced)
}}
GROUP BY ?tgn_uri # Grouping by ?tgn_uri here is for the outer query's aggregation of labels, etc.
ORDER BY ASC(SAMPLE(?final_type_rank)) ASC(SAMPLE(?min_distance_rank)) # Primary sort by type, secondary by distance
LIMIT 1
"""

# SPARQL query for Wikidata fallback
WIKIDATA_FALLBACK_QUERY_TEMPLATE = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>

SELECT DISTINCT ?wikidata_uri ?tgn_id ?wd_desc WHERE {{
  ?wikidata_uri wdt:P1667 ?tgn_id . # ?tgn_id here is the string ID, not the URI
  ?wikidata_uri skos:prefLabel ?label .
  FILTER(regex(?label, "^{search_label}$", "i")) .
  
  {{ ?wikidata_uri wdt:P131 ?top_region_entity . BIND(1 AS ?rank) }}
  UNION
  {{ ?wikidata_uri wdt:P131/wdt:P131 ?top_region_entity . BIND(2 AS ?rank) }}
  UNION
  {{ ?wikidata_uri wdt:P131/wdt:P131/wdt:P131 ?top_region_entity . BIND(3 AS ?rank) }}

  ?top_region_entity wdt:P1667 "{parent_tgn_id}" . # {parent_tgn_id} is the string ID of the parent TGN entity
  OPTIONAL {{
    ?wikidata_uri schema:description ?wd_desc .
    FILTER (lang(?wd_desc) = "en") .
  }}
}}
ORDER BY ASC(?rank)
LIMIT 1
"""

# SPARQL query to fetch TGN details by a specific TGN URI (used in fallback)
TGN_FETCH_BY_URI_QUERY_TEMPLATE = """
PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX getty: <http://vocab.getty.edu/ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX gvp: <http://vocab.getty.edu/ontology#>

SELECT 
    (SAMPLE(?label_en_coalesced) AS ?label_en) 
    (SAMPLE(?label_it_coalesced) AS ?label_it) 
    (SAMPLE(?label_de_coalesced) AS ?label_de) 
    (SAMPLE(?label_fr_coalesced) AS ?label_fr) 
    (SAMPLE(?type_term) AS ?type) 
    (SAMPLE(?scope_note_x) AS ?scope_note) 
    (SAMPLE(?label_gvp_term) AS ?label)
WHERE {{
    BIND(<{tgn_uri_direct}> AS ?tgn_uri_from_wiki) .
    OPTIONAL {{
      ?tgn_uri_from_wiki dcterms:isReplacedBy ?tgn_uri_replacement .
    }}
    BIND(COALESCE(?tgn_uri_replacement, ?tgn_uri_from_wiki) AS ?tgn_uri) .

    # English Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?enPrefLabelEntity .
      ?enPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enPrefLabelEntity getty:term ?pref_label_en .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?enAltLabelEntity .
      ?enAltLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enAltLabelEntity getty:term ?alt_label_en .
    }}
    BIND(COALESCE(?pref_label_en, ?alt_label_en) AS ?label_en_coalesced) .

    # Italian Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?itPrefLabelEntity .
      ?itPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itPrefLabelEntity getty:term ?pref_label_it .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?itAltLabelEntity .
      ?itAltLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itAltLabelEntity getty:term ?alt_label_it .
    }}
    BIND(COALESCE(?pref_label_it, ?alt_label_it) AS ?label_it_coalesced) .

    # German Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?dePrefLabelEntity .
      ?dePrefLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?dePrefLabelEntity getty:term ?pref_label_de .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?deAltLabelEntity .
      ?deAltLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?deAltLabelEntity getty:term ?alt_label_de .
    }}
    BIND(COALESCE(?pref_label_de, ?alt_label_de) AS ?label_de_coalesced) .

    # French Label (Pref or Alt)
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?frPrefLabelEntity .
      ?frPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frPrefLabelEntity getty:term ?pref_label_fr .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?frAltLabelEntity .
      ?frAltLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frAltLabelEntity getty:term ?alt_label_fr .
    }}
    BIND(COALESCE(?pref_label_fr, ?alt_label_fr) AS ?label_fr_coalesced) .

    # Getty Place Type (Preferred GVP Term)
    OPTIONAL {{
      ?tgn_uri getty:placeTypePreferred ?placeTypeEntity .
      ?placeTypeEntity getty:prefLabelGVP ?prefGVPLabelEntity .
      ?prefGVPLabelEntity getty:term ?type_term .
    }}
    
    OPTIONAL {{
      ?tgn_uri <http://www.w3.org/2004/02/skos/core#scopeNote>/rdf:value ?scope_note_x .
    }}

    # GVP Label (prefLabelGVP/term)
    OPTIONAL {{
      ?tgn_uri gvp:prefLabelGVP ?gvpLabelEntity .
      ?gvpLabelEntity gvp:term ?label_gvp_term .
    }}
}}
LIMIT 1 
"""

# SPARQL query for Wikidata second fallback (no TGN ID required for the found Wikidata entity)
WIKIDATA_SECOND_FALLBACK_QUERY_TEMPLATE = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>

SELECT DISTINCT ?wikidata_uri ?label ?wd_desc WHERE {{
  ?wikidata_uri skos:prefLabel ?label .
  FILTER(regex(?label, "^{search_label}$", "i")) .
  
  {{ ?wikidata_uri wdt:P131 ?top_region_entity . BIND(1 AS ?rank) }}
  UNION
  {{ ?wikidata_uri wdt:P131/wdt:P131 ?top_region_entity . BIND(2 AS ?rank) }}
  UNION
  {{ ?wikidata_uri wdt:P131/wdt:P131/wdt:P131 ?top_region_entity . BIND(3 AS ?rank) }}
  UNION
  {{ ?wikidata_uri wdt:P131/wdt:P131/wdt:P131/wdt:P131 ?top_region_entity . BIND(4 AS ?rank) }}
  
  ?top_region_entity wdt:P1667 "{parent_tgn_id}" .
  OPTIONAL {{
    ?wikidata_uri schema:description ?wd_desc .
    FILTER (lang(?wd_desc) = "en") .
  }}
}}
ORDER BY ASC(?rank)
LIMIT 1
"""

# SPARQL query for TGN regions - GLOBAL (no top_region_uri constraint)
GLOBAL_TGN_SPARQL_QUERY_TEMPLATE = """
PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX getty: <http://vocab.getty.edu/ontology#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX ql: <http://qlever.cs.uni-freiburg.de/builtin-functions/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX gvp: <http://vocab.getty.edu/ontology#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>

SELECT ?tgn_uri (SAMPLE(?label_en_coalesced) AS ?label_en) (SAMPLE(?label_it_coalesced) AS ?label_it) (SAMPLE(?label_de_coalesced) AS ?label_de) (SAMPLE(?label_fr_coalesced) AS ?label_fr) (SAMPLE(?type_term) AS ?type) (SAMPLE(?scope_note_x) AS ?scope_note) (SAMPLE(?label_gvp_term) AS ?label) (SAMPLE(?wikidata_uri_coalesced) AS ?wikidata_uri) (SAMPLE(?wikidata_description_coalesced) AS ?wikidata_description) WHERE {{
  # Subquery to find candidate entities and calculate their priority rank
  {{
    SELECT ?tgn_uri (MIN(?type_rank_val) AS ?final_type_rank)
    WHERE {{
        # Label matching
        ?tgn_uri skosxl:prefLabel|skosxl:altLabel ?entity .
        ?entity getty:term ?found_label_uri .
        FILTER(REGEX(?found_label_uri, "^{search_term_direct}$", "i")) .

        # Place Type Ranking for inhabited places (<http://vocab.getty.edu/aat/300008347>)
        OPTIONAL {{
            ?tgn_uri (getty:placeTypePreferred)/(getty:broaderPreferred*) <http://vocab.getty.edu/aat/300008347> .
            BIND(1 AS ?type_pref_match)
        }}
        OPTIONAL {{
            ?tgn_uri (getty:placeTypeNonPreferred)/(getty:broaderPreferred*) <http://vocab.getty.edu/aat/300008347> .
            BIND(2 AS ?type_nonpref_match)
        }}
        # FILTER(BOUND(?type_pref_match) || BOUND(?type_nonpref_match)) . # Removed to allow rank 3 for non-matches
        BIND(COALESCE(?type_pref_match, ?type_nonpref_match, 3) AS ?type_rank_val) # Assign 3 if no specific type match

    }} GROUP BY ?tgn_uri
  }}

  # Fetch details for the ranked ?tgn_uri(s) from the TGN graph
    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?enPrefLabelEntity .
      ?enPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enPrefLabelEntity getty:term ?pref_label_en .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?enAltLabelEntity .
      ?enAltLabelEntity dcterms:language <http://vocab.getty.edu/language/en> .
      ?enAltLabelEntity getty:term ?alt_label_en .
    }}
    BIND(COALESCE(?pref_label_en, ?alt_label_en) AS ?label_en_coalesced) .

    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?itPrefLabelEntity .
      ?itPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itPrefLabelEntity getty:term ?pref_label_it .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?itAltLabelEntity .
      ?itAltLabelEntity dcterms:language <http://vocab.getty.edu/language/it> .
      ?itAltLabelEntity getty:term ?alt_label_it .
    }}
    BIND(COALESCE(?pref_label_it, ?alt_label_it) AS ?label_it_coalesced) .

    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?dePrefLabelEntity .
      ?dePrefLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?dePrefLabelEntity getty:term ?pref_label_de .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?deAltLabelEntity .
      ?deAltLabelEntity dcterms:language <http://vocab.getty.edu/language/de> .
      ?deAltLabelEntity getty:term ?alt_label_de .
    }}
    BIND(COALESCE(?pref_label_de, ?alt_label_de) AS ?label_de_coalesced) .

    OPTIONAL {{
      ?tgn_uri skosxl:prefLabel ?frPrefLabelEntity .
      ?frPrefLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frPrefLabelEntity getty:term ?pref_label_fr .
    }}
    OPTIONAL {{
      ?tgn_uri skosxl:altLabel ?frAltLabelEntity .
      ?frAltLabelEntity dcterms:language <http://vocab.getty.edu/language/fr> .
      ?frAltLabelEntity getty:term ?alt_label_fr .
    }}
    BIND(COALESCE(?pref_label_fr, ?alt_label_fr) AS ?label_fr_coalesced) .

    OPTIONAL {{
      ?tgn_uri getty:placeTypePreferred ?placeTypeEntity .
      ?placeTypeEntity getty:prefLabelGVP ?prefGVPLabelEntity .
      ?prefGVPLabelEntity getty:term ?type_term .
    }}
    
    OPTIONAL {{
      ?tgn_uri <http://www.w3.org/2004/02/skos/core#scopeNote>/rdf:value ?scope_note_x .
    }}

    OPTIONAL {{
      ?tgn_uri gvp:prefLabelGVP ?gvpLabelEntity .
      ?gvpLabelEntity gvp:term ?label_gvp_term .
    }}

    ?tgn_uri dc:identifier ?tgn_id_str .
    OPTIONAL {{
      SERVICE <https://qlever.cs.uni-freiburg.de/api/wikidata> {{
        ?wd_uri wdt:P1667 ?tgn_id_str .
        OPTIONAL {{
          ?wd_uri schema:description ?wd_desc .
          FILTER (lang(?wd_desc) = "en") .
        }}
      }}
   }}
    BIND(COALESCE(?wd_uri, "") AS ?wikidata_uri_coalesced)
    BIND(COALESCE(?wd_desc, "") AS ?wikidata_description_coalesced)
}}
GROUP BY ?tgn_uri
ORDER BY ASC(SAMPLE(?final_type_rank)) # Only sort by type for global search
LIMIT 1
"""

# SPARQL query for Wikidata fallback - GLOBAL
GLOBAL_WIKIDATA_FALLBACK_QUERY_TEMPLATE = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>

SELECT DISTINCT ?wikidata_uri ?tgn_id ?wd_desc WHERE {{
  ?wikidata_uri wdt:P1667 ?tgn_id . # ?tgn_id here is the string ID, not the URI
  ?wikidata_uri skos:prefLabel ?label .
  FILTER(regex(?label, "^{search_label}$", "i")) .
  
  OPTIONAL {{
    ?wikidata_uri schema:description ?wd_desc .
    FILTER (lang(?wd_desc) = "en") .
  }}
}}
LIMIT 1 
"""

def get_sparql_binding_value(binding, key, default=""):
    # Helper to safely extract a value from a SPARQL JSON binding result.
    # A binding for a variable (key) looks like: {"type": "literal", "value": "the_actual_value"}
    # or {"type": "uri", "value": "the_uri"}
    # If the key is not present, or if its value is null (Python None) from the JSON,
    # or if the item itself is not a dictionary, return the default value.
    item = binding.get(key)
    if item and isinstance(item, dict): # Ensure item is not None and is a dictionary
        return item.get("value", default)
    return default

def parse_arguments():
    parser = argparse.ArgumentParser(description="Reconcile region names from a CSV file against the TGN SPARQL endpoint, using top-region URIs from one or more CSV definition files.")
    parser.add_argument("--regions-input-file", required=True, help="Path to the input CSV file with regions to reconcile.")
    
    parser.add_argument("--top-region-def-file", required=True, action='append', help="Path to a CSV file defining top-regions and their TGN URIs. Can be specified multiple times for different definition files.")
    parser.add_argument("--trd-name-cols", required=True, action='append', type=str, help="Comma-separated 1-based indices for the top-region name(s) in the corresponding --top-region-def-file. Must be specified for each --top-region-def-file.")
    parser.add_argument("--trd-uri-col", required=True, action='append', type=int, help="1-based column index for the top-region TGN URI in the corresponding --top-region-def-file. Must be specified for each --top-region-def-file.")
    
    parser.add_argument("--ri-top-region-name-col", required=True, type=str, help="Column index (1-based) or comma-separated indices for the top-region name(s) in the regions input file (used for lookup).")
    parser.add_argument("--ri-region-name-col", required=True, type=int, help="Column index (1-based) for the region name (term to reconcile) in the regions input file.")
    parser.add_argument("--remove-trailing-state", action='store_true', help="Remove trailing state indicators like '(XX)' from region names before querying.")
    
    args = parser.parse_args()

    if not (len(args.top_region_def_file) == len(args.trd_name_cols) == len(args.trd_uri_col)):
        parser.error("The number of --top-region-def-file, --trd-name-cols, and --trd-uri-col arguments must be the same.")

    args.top_region_configs = []
    for i in range(len(args.top_region_def_file)):
        try:
            name_cols = [int(x.strip()) - 1 for x in args.trd_name_cols[i].split(',')]
        except ValueError:
            parser.error(f"Column indices for --trd-name-cols '{args.trd_name_cols[i]}' must be integers or comma-separated integers.")
        
        args.top_region_configs.append({
            "file_path": args.top_region_def_file[i],
            "name_col_indices": name_cols,
            "uri_col_idx": args.trd_uri_col[i] - 1,
            "num_name_cols": len(name_cols)
        })

    try:
        args.ri_top_region_name_col = [int(x.strip()) - 1 for x in args.ri_top_region_name_col.split(',')]
    except ValueError:
        parser.error("Column indices for --ri-top-region-name-col must be integers or comma-separated integers.")
    
    args.ri_region_name_col -= 1

    # Sort top_region_configs by num_name_cols in descending order (most specific first)
    args.top_region_configs.sort(key=lambda x: x["num_name_cols"], reverse=True)
    
    return args

def read_top_region_definitions(top_region_configs):
    loaded_lookup_configs = []
    for config in top_region_configs:
        filename = config["file_path"]
        name_col_indices = config["name_col_indices"]
        uri_col_idx = config["uri_col_idx"]
        
        top_region_map = {}
        required_indices = name_col_indices + [uri_col_idx]
        max_req_idx = max(required_indices) if required_indices else -1

        try:
            with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader) 
                for i, row in enumerate(reader):
                    if len(row) > max_req_idx:
                        try:
                            top_region_name_parts = tuple(row[idx].strip().lower() for idx in name_col_indices)
                        except IndexError:
                            print(f"Warning: Row {i+2} in top-region definition file '{filename}' is too short for all name columns. Skipping.", file=sys.stderr)
                            continue
                            
                        top_region_uri = row[uri_col_idx].strip()
                        
                        if all(part for part in top_region_name_parts) and top_region_uri:
                            if top_region_name_parts not in top_region_map:
                                top_region_map[top_region_name_parts] = top_region_uri
                            else:
                                name_str = ", ".join(row[idx] for idx in name_col_indices)
                                print(f"Warning: Duplicate top-region name '{name_str}' found in '{filename}' on row {i+2}. Using first encountered URI.", file=sys.stderr)
                        else:
                            print(f"Warning: Missing one or more top-region name parts or URI in '{filename}' on row {i+2}. Skipping.", file=sys.stderr)
                    else:
                        print(f"Warning: Row {i+2} in '{filename}' is too short for URI or all name columns. Skipping.", file=sys.stderr)
        except FileNotFoundError:
            print(f"Error: Top-region definition file '{filename}' not found.", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Error reading top-region definition file '{filename}': {e}", file=sys.stderr)
            sys.exit(1)
            
        if not top_region_map:
            print(f"Warning: No top-region data loaded from '{filename}'. This lookup configuration might not be effective.", file=sys.stderr)
        
        loaded_lookup_configs.append({
            "map_data": top_region_map,
            "num_name_cols": config["num_name_cols"],
            "file_path": filename # For logging purposes
        })
    return loaded_lookup_configs

def read_regions_for_reconciliation(regions_filename, loaded_lookup_configs, ri_top_region_name_col_indices, region_name_col_idx, remove_trailing_state_flag): # Added remove_trailing_state_flag
    original_regions_header = []
    original_regions_data_rows = []
    sparql_values_to_query = []

    required_indices_input = ri_top_region_name_col_indices + [region_name_col_idx]
    max_req_idx_input = max(required_indices_input) if required_indices_input else -1

    try:
        with open(regions_filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            original_regions_header = next(reader)
            
            for i, row in enumerate(reader):
                original_regions_data_rows.append(row)
                if len(row) <= max_req_idx_input:
                    print(f"Warning: Row {i+2} in regions input file is too short for region name or all top-region name columns. Skipping.", file=sys.stderr)
                    continue

                try:
                    raw_top_region_parts = [row[idx].strip() for idx in ri_top_region_name_col_indices]
                except IndexError:
                    print(f"Warning: Row {i+2} in regions input file is too short for all specified top-region name columns. Skipping SPARQL query for this row.", file=sys.stderr)
                    continue
                
                # Get the original region name, strip it once for initial processing
                original_region_name_from_file = row[region_name_col_idx].strip()
                region_name_for_query = original_region_name_from_file # This will be potentially modified

                if remove_trailing_state_flag:
                    # Check for " (anything)" or "(anything)" at the end of the string
                    # The regex looks for optional whitespace, then '(', any characters (non-greedy), ')', then end of string.
                    match = re.search(r"\s*\((.*?)\)$", region_name_for_query)
                    if match:
                        # Remove the matched part (e.g., " (State)") and then strip any surrounding whitespace from the result
                        region_name_for_query = region_name_for_query[:match.start()].strip()
                
                # Clean trailing empty strings from the input top region parts
                cleaned_top_region_parts = list(raw_top_region_parts)
                while cleaned_top_region_parts and not cleaned_top_region_parts[-1]:
                    cleaned_top_region_parts.pop()
                
                final_input_key_tuple = tuple(part.lower() for part in cleaned_top_region_parts)
                
                potential_top_region_contexts = []
                if not final_input_key_tuple: # All parts were empty or no parts to begin with
                    name_str_input = ", ".join(f'"{p}"' for p in raw_top_region_parts) # Show original for clarity
                    print(f"Warning: All top-region name parts are empty for input '{name_str_input}' on data row {i+1} (file row {i+2}). Cannot find any top-region URIs.", file=sys.stderr)
                else:
                    for lookup_config in loaded_lookup_configs: # loaded_lookup_configs is already sorted by specificity
                        current_map_data = lookup_config["map_data"]
                        expected_num_cols = lookup_config["num_name_cols"]
                        
                        candidate_key = None
                        # Try to form a key for the current lookup_config
                        if len(final_input_key_tuple) >= expected_num_cols:
                            candidate_key = final_input_key_tuple[:expected_num_cols]
                        
                        if candidate_key:
                            top_region_uri = current_map_data.get(candidate_key)
                            if top_region_uri:
                                potential_top_region_contexts.append({
                                    "uri": top_region_uri,
                                    "source_file": lookup_config["file_path"],
                                    "specificity": expected_num_cols 
                                })
                                # print(f"DEBUG: Found potential context for row {i+2}: URI <{top_region_uri}> from '{lookup_config['file_path']}' (specificity {expected_num_cols}) using key {candidate_key}", file=sys.stderr)
                
                if region_name_for_query:
                    if potential_top_region_contexts:
                        sparql_values_to_query.append((region_name_for_query, potential_top_region_contexts, i)) # i is original_row_idx
                    else:
                        # No contexts found, but we still need to process this row for global search later
                        sparql_values_to_query.append((region_name_for_query, [], i)) 
                        name_str_input = ", ".join(f'"{p}"' for p in raw_top_region_parts)
                        cleaned_name_str_input = ", ".join(f'"{p}"' for p in final_input_key_tuple)
                        print(f"Info: No top-region contexts found for input (original: '{name_str_input}', cleaned: '{cleaned_name_str_input}') on data row {i+1} (file row {i+2}). Will attempt global search only.", file=sys.stderr)
                else:
                    # Log using original_region_name_from_file if region_name_for_query became empty
                    print(f"Warning: Empty region name (originally '{original_region_name_from_file}') after processing in regions input file on data row {i+1} (file row {i+2}). Skipping SPARQL query for this row.", file=sys.stderr)

    except FileNotFoundError:
        print(f"Error: Regions input file '{regions_filename}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading regions input file '{regions_filename}': {e}", file=sys.stderr)
        sys.exit(1)
        
    return original_regions_header, original_regions_data_rows, sparql_values_to_query

def extract_tgn_id_from_uri(tgn_uri):
    if not tgn_uri:
        return None
    # Regex to find numbers at the end of the TGN URI path, possibly followed by -place
    # e.g., http://vocab.getty.edu/tgn/7011781 or http://vocab.getty.edu/tgn/7011781-place
    match = re.search(r'/(\d+)(?:-place)?$', tgn_uri)
    if match:
        return match.group(1)
    # Fallback for cases where URI might be just the ID or other formats if necessary
    # For now, strict parsing of common TGN URI patterns.
    return None

def execute_generic_sparql_query(query, endpoint_url, auth_details=None, accept_header="application/sparql-results+json", timeout=300):
    headers = {
        "Accept": accept_header,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth = auth_details # Can be None for public endpoints like Wikidata

    try:
        # print(f"DEBUG: Executing Generic SPARQL Query to {endpoint_url}:\n{query}", file=sys.stderr) # Uncomment for debugging
        response = requests.post(endpoint_url, data={"query": query}, headers=headers, auth=auth, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error executing SPARQL query to {endpoint_url}: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status code: {e.response.status_code}", file=sys.stderr)
            print(f"Response text: {e.response.text}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding SPARQL JSON response from {endpoint_url}: {e}", file=sys.stderr)
        if 'response' in locals() and hasattr(response, 'text'):
             print(f"Response content: {response.text}", file=sys.stderr)
        return None

def execute_sparql_query(query): # This is the original TGN-specific one, now uses the generic executor
    auth = (SPARQL_USERNAME, SPARQL_PASSWORD)
    return execute_generic_sparql_query(query, SPARQL_ENDPOINT_URL, auth_details=auth)

def write_output_csv(original_header, original_data_rows, processed_sparql_results):
    writer = csv.writer(sys.stdout)

    script_managed_data_columns = [
        "label", "label_en", "label_it", "label_de", "label_fr", 
        "type", "scope_note", "wikidata_description", "tgn_uri", "wikidata_uri"
    ]

    # Construct final_header
    final_header = list(original_header)  # Start with a copy

    # Remove "number_of_results" if it exists in original_header, to re-add it consistently at the end
    if "number_of_results" in final_header:
        final_header.remove("number_of_results")

    # Add script-managed columns if they don't already exist in the header
    for col_name in script_managed_data_columns:
        if col_name not in final_header:
            final_header.append(col_name)
    
    # Ensure "number_of_results" is present and is the last column
    final_header.append("number_of_results")

    writer.writerow(final_header)

    # Create a map for quick index lookup in the final header
    final_header_idx_map = {name: idx for idx, name in enumerate(final_header)}

    for i, original_row_values in enumerate(original_data_rows):
        # Initialize output_row with empty strings, matching final_header length
        output_row = [""] * len(final_header)

        # Populate output_row with data from the original row
        for original_col_idx, original_col_name in enumerate(original_header):
            # Ensure we don't go out of bounds if original_row_values is shorter than original_header
            if original_col_idx < len(original_row_values):
                # If the original column name is still in our final_header map (e.g., not "number_of_results" that was removed)
                if original_col_name in final_header_idx_map:
                    target_idx = final_header_idx_map[original_col_name]
                    output_row[target_idx] = original_row_values[original_col_idx]

        sparql_matches = processed_sparql_results.get(i, [])
        num_results = len(sparql_matches)

        if sparql_matches:  # If there are reconciliation results for this row
            match = sparql_matches[0]  # Assuming LIMIT 1 logic, take the first match
            for col_name in script_managed_data_columns:
                # The column should be in final_header_idx_map due to header construction
                target_idx = final_header_idx_map[col_name]
                output_row[target_idx] = match.get(col_name, "")
        
        # Set the number_of_results value, converting to string for CSV
        output_row[final_header_idx_map["number_of_results"]] = str(num_results)

        writer.writerow(output_row)

def main():
    args = parse_arguments()

    loaded_lookup_configs = read_top_region_definitions(args.top_region_configs)
    if not any(config["map_data"] for config in loaded_lookup_configs):
        print(f"Warning: All top-region lookup maps are empty after processing the definition files. Reconciliation might not yield results.", file=sys.stderr)

    original_regions_header, original_regions_data_rows, sparql_values_to_query = \
        read_regions_for_reconciliation(args.regions_input_file, loaded_lookup_configs, args.ri_top_region_name_col, args.ri_region_name_col, args.remove_trailing_state) # Pass the flag
    
    # new_column_names_for_fallback and empty_reconciliation_fields_for_fallback are removed as this logic
    # is now handled by the improved write_output_csv function.

    if not sparql_values_to_query:
        print("No valid region/top-region combinations found to query. Outputting original data with potentially new/updated reconciliation columns.", file=sys.stderr)
        # Call the modified write_output_csv, passing an empty dict for processed_sparql_results.
        # This will ensure the header is correctly formed and original data is written with appropriate empty/zero values for reconciliation fields.
        write_output_csv(original_regions_header, original_regions_data_rows, {})
        sys.exit(0)

    processed_sparql_data = defaultdict(list)
    total_queries_to_make = len(sparql_values_to_query)
    
    print(f"Starting TGN SPARQL queries for {total_queries_to_make} regions...", file=sys.stderr)

    for i, (region_name, top_region_uri, original_row_idx) in enumerate(sparql_values_to_query):
        # Escape backslashes and double quotes for SPARQL string literal used in ql:contains-word
        escaped_region_name = region_name.replace('\\', '\\\\').replace('"', '\\"')

        query = SINGLE_REGION_TGN_SPARQL_QUERY_TEMPLATE.format(
            search_term_direct=escaped_region_name,
            top_region_uri=top_region_uri
        )
        
        print(f"Executing TGN query {i+1}/{total_queries_to_make} for region: '{region_name}', top-region TGN URI: <{top_region_uri}>", file=sys.stderr)

        sparql_response_json = execute_sparql_query(query)
        
        found_via_primary_tgn_query = False
        if sparql_response_json and "results" in sparql_response_json and "bindings" in sparql_response_json["results"]:
            bindings = sparql_response_json["results"]["bindings"]
            if len(bindings) == 1: 
                binding = bindings[0]
                try:
                    result_item = {
                        "label": get_sparql_binding_value(binding, "label"),
                        "label_en": get_sparql_binding_value(binding, "label_en"),
                        "label_it": get_sparql_binding_value(binding, "label_it"),
                        "label_de": get_sparql_binding_value(binding, "label_de"),
                        "label_fr": get_sparql_binding_value(binding, "label_fr"),
                        "type": get_sparql_binding_value(binding, "type"),
                        "scope_note": get_sparql_binding_value(binding, "scope_note"),
                        "wikidata_description": get_sparql_binding_value(binding, "wikidata_description"),
                        "tgn_uri": get_sparql_binding_value(binding, "tgn_uri"),
                        "wikidata_uri": get_sparql_binding_value(binding, "wikidata_uri"),
                    }
                    if not result_item["tgn_uri"]:
                        print(f"Warning: Primary TGN query for '{region_name}' succeeded but ?tgn_uri is missing in result. Binding: {binding}", file=sys.stderr)
                    else:
                        processed_sparql_data[original_row_idx].append(result_item)
                        found_via_primary_tgn_query = True
                except KeyError as e: 
                    print(f"Warning: Error processing binding for '{region_name}' from primary TGN query. Binding: {binding}. Error: {e}", file=sys.stderr)
            
            elif len(bindings) == 0:
                # This is where we will trigger fallback, so message will be handled below if not found_via_primary_tgn_query
                pass # No primary TGN match found, will proceed to fallback.
            
            else: # More than 1 result from primary TGN query
                print(f"Warning: Primary TGN Query for '{region_name}' returned {len(bindings)} results, expected 0 or 1 due to LIMIT 1. Using first result if available, but this is unexpected. Bindings: {bindings}", file=sys.stderr)
                if get_sparql_binding_value(bindings[0], "tgn_uri"): # Check if tgn_uri has a value
                     binding = bindings[0]
                     result_item = {
                        "label": get_sparql_binding_value(binding, "label"),
                        "label_en": get_sparql_binding_value(binding, "label_en"),
                        "label_it": get_sparql_binding_value(binding, "label_it"),
                        "label_de": get_sparql_binding_value(binding, "label_de"),
                        "label_fr": get_sparql_binding_value(binding, "label_fr"),
                        "type": get_sparql_binding_value(binding, "type"),
                        "scope_note": get_sparql_binding_value(binding, "scope_note"),
                        "wikidata_description": get_sparql_binding_value(binding, "wikidata_description"),
                        "tgn_uri": get_sparql_binding_value(binding, "tgn_uri"),
                        "wikidata_uri": get_sparql_binding_value(binding, "wikidata_uri"),
                    }
                     processed_sparql_data[original_row_idx].append(result_item)
                     found_via_primary_tgn_query = True
        # else: # Primary TGN Query failed or returned malformed/empty data
            # Fallback will be attempted if found_via_primary_tgn_query is still False

        if not found_via_primary_tgn_query:
            print(f"Info: No definitive TGN match for '{region_name}' (top-region: <{top_region_uri}>) via primary query. Attempting Wikidata fallback.", file=sys.stderr)
            parent_tgn_id = extract_tgn_id_from_uri(top_region_uri)

            if parent_tgn_id:
                # Use the same escaped_region_name for Wikidata query's search_label
                # The WIKIDATA_FALLBACK_QUERY_TEMPLATE uses "^{search_label}$" for regex.
                wikidata_query = WIKIDATA_FALLBACK_QUERY_TEMPLATE.format(
                    search_label=escaped_region_name, # Use the already escaped name
                    parent_tgn_id=parent_tgn_id
                )
                
                print(f"Executing Wikidata fallback query for '{region_name}', parent TGN ID: {parent_tgn_id}", file=sys.stderr)
                wikidata_response_json = execute_generic_sparql_query(wikidata_query, WIKIDATA_SPARQL_ENDPOINT_URL)

                if wikidata_response_json and "results" in wikidata_response_json and "bindings" in wikidata_response_json["results"]:
                    wd_bindings = wikidata_response_json["results"]["bindings"]
                    if len(wd_bindings) == 1:
                        wd_binding = wd_bindings[0]
                        fallback_tgn_id_str = wd_binding.get("tgn_id", {}).get("value", "") # This is the TGN ID string
                        fallback_wikidata_uri = wd_binding.get("wikidata_uri", {}).get("value", "")
                        fallback_wikidata_desc = wd_binding.get("wd_desc", {}).get("value", "")

                        if fallback_tgn_id_str and fallback_wikidata_uri:
                            tgn_uri_from_wikidata = f"http://vocab.getty.edu/tgn/{fallback_tgn_id_str}"
                            
                            print(f"Wikidata fallback found TGN ID: {fallback_tgn_id_str}, Wikidata URI: <{fallback_wikidata_uri}>. Fetching details from TGN for <{tgn_uri_from_wikidata}>.", file=sys.stderr)

                            tgn_details_query = TGN_FETCH_BY_URI_QUERY_TEMPLATE.format(tgn_uri_direct=tgn_uri_from_wikidata)
                            # Execute this query against the TGN endpoint
                            tgn_details_response_json = execute_sparql_query(tgn_details_query) 

                            if tgn_details_response_json and "results" in tgn_details_response_json and "bindings" in tgn_details_response_json["results"]:
                                tgn_details_bindings = tgn_details_response_json["results"]["bindings"]
                                if len(tgn_details_bindings) == 1:
                                    tgn_detail_binding = tgn_details_bindings[0]
                                    fallback_result_item = {
                                        "label": get_sparql_binding_value(tgn_detail_binding, "label"),
                                        "label_en": get_sparql_binding_value(tgn_detail_binding, "label_en"),
                                        "label_it": get_sparql_binding_value(tgn_detail_binding, "label_it"),
                                        "label_de": get_sparql_binding_value(tgn_detail_binding, "label_de"),
                                        "label_fr": get_sparql_binding_value(tgn_detail_binding, "label_fr"),
                                        "type": get_sparql_binding_value(tgn_detail_binding, "type"),
                                        "scope_note": get_sparql_binding_value(tgn_detail_binding, "scope_note"),
                                        "wikidata_description": fallback_wikidata_desc, # From Wikidata query
                                        "tgn_uri": tgn_uri_from_wikidata, # The one we just looked up
                                        "wikidata_uri": fallback_wikidata_uri, # From Wikidata query
                                    }
                                    processed_sparql_data[original_row_idx].append(fallback_result_item)
                                    print(f"Successfully processed TGN details via Wikidata fallback for '{region_name}'.", file=sys.stderr)
                                else:
                                    print(f"Warning: TGN details fetch (via Wikidata fallback) for TGN URI <{tgn_uri_from_wikidata}> returned {len(tgn_details_bindings)} results (expected 1) or no bindings. No data added for fallback.", file=sys.stderr)
                            else:
                                print(f"Warning: Failed to fetch TGN details (via Wikidata fallback) for TGN URI <{tgn_uri_from_wikidata}> (query failed or malformed response). No data added for fallback.", file=sys.stderr)
                        else:
                            print(f"Info: Wikidata fallback query for '{region_name}' did not return a TGN ID or Wikidata URI. wd_binding: {wd_binding}", file=sys.stderr)
                    elif len(wd_bindings) == 0:
                        print(f"Info: Wikidata fallback query for '{region_name}' returned no results.", file=sys.stderr)
                    else: # More than 1 result from Wikidata
                        print(f"Warning: Wikidata fallback query for '{region_name}' returned {len(wd_bindings)} results. Expected 0 or 1. No fallback action taken.", file=sys.stderr)
                else:
                    print(f"Warning: Wikidata fallback query failed or returned malformed/empty data for '{region_name}'.", file=sys.stderr)
            else:
                print(f"Warning: Could not extract parent TGN ID from <{top_region_uri}> for Wikidata fallback for region '{region_name}'. Fallback skipped.", file=sys.stderr)
            
            # If still no result after first fallback, try second fallback (Wikidata only, no TGN ID needed for match)
            if not processed_sparql_data[original_row_idx] and parent_tgn_id: # Check parent_tgn_id again, though it should be set if first fallback was attempted
                print(f"Info: First Wikidata fallback for '{region_name}' did not yield a TGN record. Attempting second Wikidata fallback (Wikidata entity only).", file=sys.stderr)
                
                second_wikidata_query = WIKIDATA_SECOND_FALLBACK_QUERY_TEMPLATE.format(
                    search_label=escaped_region_name,
                    parent_tgn_id=parent_tgn_id
                )
                print(f"Executing second Wikidata fallback query for '{region_name}', parent TGN ID: {parent_tgn_id}", file=sys.stderr)
                second_wikidata_response_json = execute_generic_sparql_query(second_wikidata_query, WIKIDATA_SPARQL_ENDPOINT_URL)

                if second_wikidata_response_json and "results" in second_wikidata_response_json and "bindings" in second_wikidata_response_json["results"]:
                    swd_bindings = second_wikidata_response_json["results"]["bindings"]
                    if len(swd_bindings) == 1:
                        swd_binding = swd_bindings[0]
                        second_fallback_wikidata_uri = swd_binding.get("wikidata_uri", {}).get("value", "")
                        second_fallback_label = swd_binding.get("label", {}).get("value", "") # This is skos:prefLabel
                        second_fallback_wikidata_desc = swd_binding.get("wd_desc", {}).get("value", "")

                        if second_fallback_wikidata_uri and second_fallback_label:
                            second_fallback_result_item = {
                                "label": second_fallback_label, # Use Wikidata label
                                "label_en": "", # No TGN data
                                "label_it": "",
                                "label_de": "",
                                "label_fr": "",
                                "type": "",
                                "scope_note": "",
                                "wikidata_description": second_fallback_wikidata_desc,
                                "tgn_uri": "", # No TGN URI from this fallback
                                "wikidata_uri": second_fallback_wikidata_uri,
                            }
                            processed_sparql_data[original_row_idx].append(second_fallback_result_item)
                            print(f"Successfully processed Wikidata-only fallback for '{region_name}'. Wikidata URI: <{second_fallback_wikidata_uri}>", file=sys.stderr)
                        else:
                            print(f"Info: Second Wikidata fallback query for '{region_name}' did not return a complete wikidata_uri and label. swd_binding: {swd_binding}", file=sys.stderr)
                    elif len(swd_bindings) == 0:
                        print(f"Info: Second Wikidata fallback query for '{region_name}' returned no results.", file=sys.stderr)
                    else: # More than 1 result
                        print(f"Warning: Second Wikidata fallback query for '{region_name}' returned {len(swd_bindings)} results. Expected 0 or 1. No action taken.", file=sys.stderr)
                else:
                    print(f"Warning: Second Wikidata fallback query failed or returned malformed/empty data for '{region_name}'.", file=sys.stderr)
            elif not processed_sparql_data[original_row_idx] and not parent_tgn_id:
                 print(f"Info: Cannot attempt second Wikidata fallback for '{region_name}' as parent_tgn_id was not extracted.", file=sys.stderr)


        # else: Successfully found via primary TGN query, no fallback needed.

    print(f"Finished TGN and potential Wikidata fallback SPARQL queries for {total_queries_to_make} regions.", file=sys.stderr)
def process_and_store_tgn_match(sparql_response_json, region_name, original_row_idx, processed_sparql_data, context_label=""):
    """
    Processes SPARQL response from a TGN query (contextual or global) and stores the match if found.
    Returns True if a match was successfully processed and stored, False otherwise.
    """
    if sparql_response_json and "results" in sparql_response_json and "bindings" in sparql_response_json["results"]:
        bindings = sparql_response_json["results"]["bindings"]
        if len(bindings) >= 1: # Expect 0 or 1 due to LIMIT 1, but handle >=1 defensively
            if len(bindings) > 1:
                print(f"Warning: TGN Query ({context_label}) for '{region_name}' returned {len(bindings)} results, expected 0 or 1. Using first result.", file=sys.stderr)
            
            binding = bindings[0]
            try:
                result_item = {
                    "label": get_sparql_binding_value(binding, "label"),
                    "label_en": get_sparql_binding_value(binding, "label_en"),
                    "label_it": get_sparql_binding_value(binding, "label_it"),
                    "label_de": get_sparql_binding_value(binding, "label_de"),
                    "label_fr": get_sparql_binding_value(binding, "label_fr"),
                    "type": get_sparql_binding_value(binding, "type"),
                    "scope_note": get_sparql_binding_value(binding, "scope_note"),
                    "wikidata_description": get_sparql_binding_value(binding, "wikidata_description"),
                    "tgn_uri": get_sparql_binding_value(binding, "tgn_uri"),
                    "wikidata_uri": get_sparql_binding_value(binding, "wikidata_uri"),
                }
                if not result_item["tgn_uri"]:
                    print(f"Warning: TGN query ({context_label}) for '{region_name}' succeeded but ?tgn_uri is missing. Binding: {binding}", file=sys.stderr)
                    return False
                else:
                    processed_sparql_data[original_row_idx].append(result_item)
                    print(f"Success: Found TGN match for '{region_name}' via {context_label} query. TGN URI: <{result_item['tgn_uri']}>", file=sys.stderr)
                    return True
            except KeyError as e: 
                print(f"Warning: Error processing binding for '{region_name}' from TGN query ({context_label}). Binding: {binding}. Error: {e}", file=sys.stderr)
                return False
        # else len(bindings) == 0, no match found
    # else: query failed or malformed response
    return False

def attempt_wikidata_fallbacks(escaped_region_name, parent_tgn_id_for_context, original_row_idx, processed_sparql_data, context_label=""):
    """
    Attempts Wikidata fallbacks (first with TGN ID, then Wikidata entity only).
    Uses contextual or global templates based on whether parent_tgn_id_for_context is provided.
    Returns True if any fallback succeeded, False otherwise.
    """
    # Determine if this is a contextual or global fallback
    is_global_fallback = parent_tgn_id_for_context is None

    # --- First Wikidata Fallback (expects TGN ID on Wikidata entity) ---
    if is_global_fallback:
        wikidata_query_template = GLOBAL_WIKIDATA_FALLBACK_QUERY_TEMPLATE
        wd_query_params = {"search_label": escaped_region_name}
        print(f"Executing Global Wikidata fallback (1st type) for '{escaped_region_name}'", file=sys.stderr)
    elif parent_tgn_id_for_context:
        wikidata_query_template = WIKIDATA_FALLBACK_QUERY_TEMPLATE
        wd_query_params = {"search_label": escaped_region_name, "parent_tgn_id": parent_tgn_id_for_context}
        print(f"Executing Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}', parent TGN ID: {parent_tgn_id_for_context}", file=sys.stderr)
    else: # Contextual fallback but no parent_tgn_id (e.g. top_region_uri was invalid)
        print(f"Info: Skipping Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}' as parent_tgn_id is missing.", file=sys.stderr)
        return False # Cannot proceed with this type of fallback

    wikidata_query = wikidata_query_template.format(**wd_query_params)
    wikidata_response_json = execute_generic_sparql_query(wikidata_query, WIKIDATA_SPARQL_ENDPOINT_URL)

    if wikidata_response_json and "results" in wikidata_response_json and "bindings" in wikidata_response_json["results"]:
        wd_bindings = wikidata_response_json["results"]["bindings"]
        if len(wd_bindings) == 1:
            wd_binding = wd_bindings[0]
            fallback_tgn_id_str = get_sparql_binding_value(wd_binding, "tgn_id")
            fallback_wikidata_uri = get_sparql_binding_value(wd_binding, "wikidata_uri")
            fallback_wikidata_desc = get_sparql_binding_value(wd_binding, "wd_desc")

            if fallback_tgn_id_str and fallback_wikidata_uri:
                tgn_uri_from_wikidata = f"http://vocab.getty.edu/tgn/{fallback_tgn_id_str}"
                print(f"Wikidata fallback (1st type, {context_label}) found TGN ID: {fallback_tgn_id_str}, Wikidata URI: <{fallback_wikidata_uri}>. Fetching TGN details for <{tgn_uri_from_wikidata}>.", file=sys.stderr)

                tgn_details_query = TGN_FETCH_BY_URI_QUERY_TEMPLATE.format(tgn_uri_direct=tgn_uri_from_wikidata)
                tgn_details_response_json = execute_sparql_query(tgn_details_query) # TGN specific auth

                if tgn_details_response_json and "results" in tgn_details_response_json and "bindings" in tgn_details_response_json["results"]:
                    tgn_details_bindings = tgn_details_response_json["results"]["bindings"]
                    if len(tgn_details_bindings) == 1:
                        tgn_detail_binding = tgn_details_bindings[0]
                        fallback_result_item = {
                            "label": get_sparql_binding_value(tgn_detail_binding, "label"),
                            "label_en": get_sparql_binding_value(tgn_detail_binding, "label_en"),
                            "label_it": get_sparql_binding_value(tgn_detail_binding, "label_it"),
                            "label_de": get_sparql_binding_value(tgn_detail_binding, "label_de"),
                            "label_fr": get_sparql_binding_value(tgn_detail_binding, "label_fr"),
                            "type": get_sparql_binding_value(tgn_detail_binding, "type"),
                            "scope_note": get_sparql_binding_value(tgn_detail_binding, "scope_note"),
                            "wikidata_description": fallback_wikidata_desc,
                            "tgn_uri": tgn_uri_from_wikidata,
                            "wikidata_uri": fallback_wikidata_uri,
                        }
                        processed_sparql_data[original_row_idx].append(fallback_result_item)
                        print(f"Success: Processed TGN details via Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}'.", file=sys.stderr)
                        return True
                    else:
                        print(f"Warning: TGN details fetch (via Wikidata fallback 1st type, {context_label}) for TGN URI <{tgn_uri_from_wikidata}> returned {len(tgn_details_bindings)} results. No data added.", file=sys.stderr)
                else:
                    print(f"Warning: Failed to fetch TGN details (via Wikidata fallback 1st type, {context_label}) for TGN URI <{tgn_uri_from_wikidata}>. No data added.", file=sys.stderr)
            else:
                print(f"Info: Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}' did not return TGN ID or Wikidata URI. wd_binding: {wd_binding}", file=sys.stderr)
        elif len(wd_bindings) == 0:
            print(f"Info: Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}' returned no results.", file=sys.stderr)
        else:
            print(f"Warning: Wikidata fallback (1st type, {context_label}) for '{escaped_region_name}' returned {len(wd_bindings)} results. No action.", file=sys.stderr)
    else:
        print(f"Warning: Wikidata fallback (1st type, {context_label}) query failed or malformed for '{escaped_region_name}'.", file=sys.stderr)

    # --- Second Wikidata Fallback (Wikidata entity only, no TGN ID needed for match) ---
    # Global version of this fallback has been removed as per user request.
    if not is_global_fallback:
        if parent_tgn_id_for_context: # This check is important, as the contextual query needs parent_tgn_id
            second_wikidata_query_template = WIKIDATA_SECOND_FALLBACK_QUERY_TEMPLATE
            second_wd_query_params = {"search_label": escaped_region_name, "parent_tgn_id": parent_tgn_id_for_context}
            print(f"Executing Wikidata fallback (2nd type, {context_label}) for '{escaped_region_name}', parent TGN ID: {parent_tgn_id_for_context}", file=sys.stderr)
        else: # Contextual fallback but no parent_tgn_id
            print(f"Info: Skipping Wikidata fallback (2nd type, {context_label}) for '{escaped_region_name}' as parent_tgn_id is missing.", file=sys.stderr)
            return False # Cannot proceed with this type of fallback

        second_wikidata_query = second_wikidata_query_template.format(**second_wd_query_params)
        second_wikidata_response_json = execute_generic_sparql_query(second_wikidata_query, WIKIDATA_SPARQL_ENDPOINT_URL)

        if second_wikidata_response_json and "results" in second_wikidata_response_json and "bindings" in second_wikidata_response_json["results"]:
            swd_bindings = second_wikidata_response_json["results"]["bindings"]
            if len(swd_bindings) == 1:
                swd_binding = swd_bindings[0]
                second_fallback_wikidata_uri = get_sparql_binding_value(swd_binding, "wikidata_uri")
                second_fallback_label = get_sparql_binding_value(swd_binding, "label") # skos:prefLabel
                second_fallback_wikidata_desc = get_sparql_binding_value(swd_binding, "wd_desc")

                if second_fallback_wikidata_uri and second_fallback_label:
                    second_fallback_result_item = {
                        "label": second_fallback_label, "label_en": "", "label_it": "", "label_de": "", "label_fr": "",
                        "type": "", "scope_note": "", "wikidata_description": second_fallback_wikidata_desc,
                        "tgn_uri": "", "wikidata_uri": second_fallback_wikidata_uri,
                    }
                    processed_sparql_data[original_row_idx].append(second_fallback_result_item)
                    print(f"Success: Processed Wikidata-only fallback (2nd type, {context_label}) for '{escaped_region_name}'. Wikidata URI: <{second_fallback_wikidata_uri}>", file=sys.stderr)
                    return True
                else:
                    print(f"Info: Wikidata fallback (2nd type, {context_label}) for '{escaped_region_name}' did not return wikidata_uri and label. swd_binding: {swd_binding}", file=sys.stderr)
            elif len(swd_bindings) == 0:
                print(f"Info: Wikidata fallback (2nd type, {context_label}) for '{escaped_region_name}' returned no results.", file=sys.stderr)
            else:
                print(f"Warning: Wikidata fallback (2nd type, {context_label}) for '{escaped_region_name}' returned {len(swd_bindings)} results. No action.", file=sys.stderr)
        else:
            print(f"Warning: Wikidata fallback (2nd type, {context_label}) query failed or malformed for '{escaped_region_name}'.", file=sys.stderr)
    else: # is_global_fallback is true
        print(f"Info: Global Wikidata fallback (2nd type) for '{escaped_region_name}' was removed by user request. Skipping.", file=sys.stderr)
        
    return False


def main():
    args = parse_arguments()

    # loaded_lookup_configs is already sorted by specificity (num_name_cols desc) by parse_arguments
    loaded_lookup_configs = read_top_region_definitions(args.top_region_configs)
    if not any(config["map_data"] for config in loaded_lookup_configs) and args.top_region_def_file: # Check if def files were given but all empty
        print(f"Warning: All top-region lookup maps are empty after processing definition files. Only global search will be effective if no contexts are found per item.", file=sys.stderr)

    original_regions_header, original_regions_data_rows, sparql_values_to_query = \
        read_regions_for_reconciliation(args.regions_input_file, loaded_lookup_configs, args.ri_top_region_name_col, args.ri_region_name_col, args.remove_trailing_state)
    
    if not sparql_values_to_query:
        print("No regions to query based on input. Outputting original data with potentially new/updated reconciliation columns.", file=sys.stderr)
        write_output_csv(original_regions_header, original_regions_data_rows, {})
        sys.exit(0)

    processed_sparql_data = defaultdict(list)
    total_items_to_reconcile = len(sparql_values_to_query)
    
    print(f"Starting reconciliation for {total_items_to_reconcile} regions...", file=sys.stderr)

    for item_idx, (region_name, potential_top_region_contexts, original_row_idx) in enumerate(sparql_values_to_query):
        print(f"\nProcessing item {item_idx+1}/{total_items_to_reconcile}: '{region_name}' (Original Row Index: {original_row_idx})", file=sys.stderr)
        escaped_region_name = region_name.replace('\\', '\\\\').replace('"', '\\"')
        match_found_for_row = False

        # --- Hierarchical Context Search ---
        if potential_top_region_contexts:
            print(f"Attempting hierarchical search with {len(potential_top_region_contexts)} context(s) for '{region_name}'.", file=sys.stderr)
            for context_info in potential_top_region_contexts:
                current_top_region_uri = context_info["uri"]
                context_label = f"contextual (source: {context_info['source_file']}, specificity: {context_info['specificity']})"
                
                print(f"  Trying TGN search for '{region_name}' with top-region <{current_top_region_uri}> ({context_label})", file=sys.stderr)
                query = SINGLE_REGION_TGN_SPARQL_QUERY_TEMPLATE.format(
                    search_term_direct=escaped_region_name,
                    top_region_uri=current_top_region_uri
                )
                sparql_response_json = execute_sparql_query(query)
                if process_and_store_tgn_match(sparql_response_json, region_name, original_row_idx, processed_sparql_data, context_label="TGN " + context_label):
                    match_found_for_row = True
                    break # Found a match, move to next region_name

                # If TGN contextual search failed for this context, try Wikidata fallbacks for THIS context
                print(f"  TGN search failed for context <{current_top_region_uri}>. Attempting Wikidata fallbacks for this context.", file=sys.stderr)
                parent_tgn_id = extract_tgn_id_from_uri(current_top_region_uri)
                if attempt_wikidata_fallbacks(escaped_region_name, parent_tgn_id, original_row_idx, processed_sparql_data, context_label="Wikidata " + context_label):
                    match_found_for_row = True
                    break # Found a match, move to next region_name
            
            if match_found_for_row:
                continue # To next item in sparql_values_to_query
        else:
            print(f"No hierarchical contexts found for '{region_name}'. Proceeding to global search.", file=sys.stderr)


        # --- Global Search Stage (if no match found in hierarchical contexts) ---
        if not match_found_for_row:
            print(f"Hierarchical search failed or no contexts for '{region_name}'. Attempting global search.", file=sys.stderr)
            
            # Global TGN Search
            print(f"  Trying Global TGN search for '{region_name}'", file=sys.stderr)
            global_tgn_query = GLOBAL_TGN_SPARQL_QUERY_TEMPLATE.format(search_term_direct=escaped_region_name)
            sparql_response_json = execute_sparql_query(global_tgn_query)
            if process_and_store_tgn_match(sparql_response_json, region_name, original_row_idx, processed_sparql_data, context_label="TGN Global"):
                match_found_for_row = True
            
            if not match_found_for_row:
                # Global Wikidata Fallbacks (parent_tgn_id_for_context is None for global)
                print(f"  Global TGN search failed for '{region_name}'. Attempting Global Wikidata fallbacks.", file=sys.stderr)
                if attempt_wikidata_fallbacks(escaped_region_name, None, original_row_idx, processed_sparql_data, context_label="Wikidata Global"):
                    match_found_for_row = True

        if not match_found_for_row:
            print(f"Exhausted all search methods for '{region_name}'. No match found.", file=sys.stderr)
        # else: match was found at some stage.

    print(f"\nFinished all reconciliation attempts.", file=sys.stderr)
    
    write_output_csv(original_regions_header, original_regions_data_rows, processed_sparql_data)

if __name__ == "__main__":
    main()
