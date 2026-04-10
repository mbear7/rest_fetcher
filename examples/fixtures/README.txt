Example playback fixtures

This directory contains curated raw playback fixtures for a subset of examples in examples.py.

Use them by changing the matching example's playback block from:
  'mode': 'none'
to:
  'mode': 'load'

Files:
- atom_directory_example.json                -> atom_example.py (two-page Atom/XML directory)
- example_02_list_events.json              -> example 2, cursor pagination
- example_05_custom_pagination_state.json  -> example 5, custom pagination with state
- example_18_offset_users.json             -> example 18, offset pagination
- example_18_page_number_posts.json        -> example 18, page-number pagination
- example_19_link_header_repos.json        -> example 19, Link-header pagination
- example_26_playback_users.json           -> example 26, playback basics
- example_35_text_status.json              -> example 35, text response
- example_36_xml_feed.json                 -> example 36, XML response
- example_37_csv_report.json               -> example 37, CSV response
- example_38_nested_pagination.json        -> example 38, nested-path pagination
- example_39_callback_debugging.json       -> example 39, raw playback callback debugging

Maintenance rule:
Treat each curated example + fixture pair as a linked asset.
When changing a curated example, verify that its fixture still matches and update the
corresponding replay/scenario test if needed.

Note: the pcal case-study fixture lives under examples/case_studies/fixtures/.
