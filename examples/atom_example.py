'''
Generic example: non-JSON pagination with `canonical_parser` + playback.

This is intentionally small and neutral (Atom/XML-ish), so you can see the pattern
without any domain-specific baggage.

Run from the repo root:
    python examples/atom_example.py
'''

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

# Allow running as a script from the repo root.
try:
    ROOT = Path(__file__).resolve().parents[1]
except NameError:  # running in a notebook / REPL
    ROOT = Path.cwd().resolve()
    for _ in range(6):
        if (ROOT / 'rest_fetcher').is_dir():
            break
        ROOT = ROOT.parent

sys.path.insert(0, str(ROOT))


from rest_fetcher import APIClient  # noqa: E402


ATOM_NS = {'a': 'http://www.w3.org/2005/Atom'}


@dataclass(frozen=True)
class Entry:
    id: str
    title: str
    updated: str
    url: str


def canonical_parser(content: bytes, context: dict[str, Any]) -> ET.Element:
    # Canonical payload for pagination: an ElementTree root.
    return ET.fromstring(content)


def update_state(page_payload: ET.Element, state: dict[str, Any]) -> dict[str, Any] | None:
    # State-driven pagination after discovery.
    # State is read-only here; return a dict of updates to persist changes.
    if not state.get('did_discover'):
        link = page_payload.find(".//a:link[@rel='related']", ATOM_NS)
        if link is None:
            raise RuntimeError('directory page missing related link')
        href = (link.attrib.get('href') or '').strip()
        if not href:
            raise RuntimeError('directory page has empty related href')
        return {'queue': [href], 'queue_i': 0, 'did_discover': True}

    queue = list(state.get('queue') or [])
    qi = int(state.get('queue_i') or 0)
    if qi < len(queue):
        return {'queue_i': qi + 1}
    return None

def next_request(parsed_body: ET.Element, state: dict[str, Any]) -> dict[str, Any] | None:
    # Navigation is state-driven after discovery. `parsed_body` is available but
    # not required here. Not every callback must use every argument.
    queue = list(state.get('queue') or [])
    qi = int(state.get('queue_i') or 0)
    if qi >= len(queue):
        return None
    return {'url': queue[qi]}

def main() -> None:
    fixture_path = ROOT / 'examples' / 'fixtures' / 'atom_directory_example.json'
    entries: list[Entry] = []

    def on_page(page_data: ET.Element, state: dict[str, Any]) -> None:
        # Directory page has entries without <id>. Category page entries include <id>.
        # This keeps the example independent of any specific state keys.
        for entry_el in page_data.findall('.//a:entry', ATOM_NS):
            entry_id = (entry_el.findtext('a:id', default='', namespaces=ATOM_NS) or '').strip()
            if not entry_id:
                continue
            title = (entry_el.findtext('a:title', default='', namespaces=ATOM_NS) or '').strip()
            updated = (entry_el.findtext('a:updated', default='', namespaces=ATOM_NS) or '').strip()
            link_el = entry_el.find("a:link[@rel='alternate']", ATOM_NS)
            url = (link_el.attrib.get('href') if link_el is not None else '').strip()
            entries.append(Entry(entry_id, title, updated, url))

    client = APIClient({
        'base_url': 'https://example.test',
        'endpoints': {
            'atom': {
                'method': 'GET',
                'path': '/atom/directory',
                'response_format': 'xml',
                'pagination': {
                    'next_request': next_request,
                },
                'update_state': update_state,
                'canonical_parser': canonical_parser,
                'on_page': on_page,
                'playback': {'mode': 'load', 'path': str(fixture_path)},
            }
        }
    })

    run = client.stream_run('atom')
    for _ in run:
        pass

    for e in entries:
        print(f'{e.updated} | {e.title} | {e.url}')

    print('Entries:', len(entries))
    print('Summary:', run.summary)

if __name__ == '__main__':
    main()
