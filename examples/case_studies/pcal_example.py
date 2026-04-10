'''
Illustrative example: non-JSON pagination using XPath.

This library is not a web-scraping framework. This script exists to show how
`canonical_parser` + `next_request(parsed_body, state)` make pagination work for
HTML/XML-ish endpoints without re-parsing.

Run from the directory containing rest_fetcher/:
    python examples/case_studies/pcal_example.py

Requires:
    pip install lxml

This is a real (anonymized) production-style integration pattern.
Don’t focus on 'Consultant' or 'calendar'. Focus on:
- XML parsing with lxml + XPath (via canonical_parser)
- Pagination driven by parsed XML elements (parsed_body)
- Byte-faithful recording (playback.record_as_bytes=True) for exact XML fidelity
The committed fixture makes it run offline by default.
'''



from __future__ import annotations

import csv
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

try:
    ROOT = Path(__file__).resolve().parents[1]
except NameError:  # running in a notebook / REPL
    # assume CWD is somewhere inside the repo
    ROOT = Path.cwd().resolve()
    for _ in range(6):
        if (ROOT / 'rest_fetcher').is_dir():
            break
        ROOT = ROOT.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rest_fetcher import APIClient

try:
    from lxml import html  # type: ignore[import-not-found]
except Exception:
    html = None


ORIGIN = 'https://www.consultant.ru'
BASE_PATH = '/law/ref/calendar/proizvodstvennye/'
BASE_URL = urljoin(ORIGIN, BASE_PATH)
FIXTURE_PATH = 'examples/case_studies/fixtures/consultant_pcal_2023-2026.json'

_MONTHS_RU = {
    1: 'январь',
    2: 'февраль',
    3: 'март',
    4: 'апрель',
    5: 'май',
    6: 'июнь',
    7: 'июль',
    8: 'август',
    9: 'сентябрь',
    10: 'октябрь',
    11: 'ноябрь',
    12: 'декабрь',
}

# Example-scoped month mapping. Production code uses Babel, but this example
# keeps the dependency surface narrow.
_MONTH_MAP = {v.casefold(): k for k, v in _MONTHS_RU.items()}

# Production semantics: empty kind + work + preholiday are treated as working days.
_WORKING_KINDS = frozenset({'', 'preholiday', 'work'})


@dataclass(frozen=True)
class CalendarDay:
    dt: date
    kind: str


def _canonical_lxml_html(content: bytes, _context: dict[str, Any]) -> Any:
    if html is None:
        raise RuntimeError('This example requires lxml. Install it: pip install lxml')
    return html.fromstring(content)


def _extract_year_links(tree: Any) -> list[tuple[int, str]]:
    links: list[tuple[int, str]] = []

    # Production-derived XPath: year comes from link text, while href may carry a suffix.
    for a in tree.xpath("//ul[contains(@class,'list-inline')]//li/a"):
        href = (a.get('href') or '').strip()
        txt = (a.text_content() or '').strip()
        if not href or not txt.isdigit():
            continue
        links.append((int(txt), urljoin(ORIGIN, href)))

    links.sort(key=lambda item: item[0])
    return links


def _parse_month(table: Any, year: int) -> Iterable[CalendarDay]:
    month_name = table.xpath("string(.//th[contains(@class,'month')])").strip().casefold()
    if not month_name:
        return

    month = _MONTH_MAP.get(month_name)
    if month is None:
        return

    for cell in table.xpath('.//td'):
        kind = (cell.get('class') or '').strip()
        if kind == 'inactively':
            continue

        raw = cell.xpath('normalize-space(.)').rstrip('*')
        if raw.isdigit():
            yield CalendarDay(date(year, month, int(raw)), kind)


def _extract_year(tree: Any, year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for table in tree.xpath("//table[contains(@class,'cal')]"):
        for rec in _parse_month(table, year):
            rows.append({
                'date': f'{rec.dt:%Y-%m-%d}',
                'month_ru': _MONTHS_RU.get(rec.dt.month, ''),
                'kind': rec.kind,
                'working': int(rec.kind.casefold() in _WORKING_KINDS),
                'year': year,
            })

    return rows


def update_state(page_payload: Any, state: dict[str, Any]) -> dict[str, Any] | None:
    if 'years' not in state:
        year_from = int(state['year_from'])
        year_to = int(state['year_to'])

        discovered = _extract_year_links(page_payload)
        years = [(year, url) for year, url in discovered if year_from <= year <= year_to]
        if not years:
            raise RuntimeError('Failed to extract year links (site markup changed?)')

        return {
            'years': years,
            'year_idx': 0,
        }

    if page_payload.xpath("//table[contains(@class,'cal')]"):
        return {'year_idx': int(state.get('year_idx', 0)) + 1}

    return None


def next_request(parsed_body: Any, state: dict[str, Any]) -> dict[str, Any] | None:
    years: list[tuple[int, str]] | None = state.get('years')
    if not years:
        # The initial index-page request comes from the endpoint schema.
        return None

    idx = int(state.get('year_idx', 0))
    if idx >= len(years):
        return None

    _year, url = years[idx]
    return {'url': url}


def on_response(page_payload: Any, state: dict[str, Any]) -> list[dict[str, Any]]:
    years: list[tuple[int, str]] | None = state.get('years')
    if not years:
        return []

    idx = int(state.get('year_idx', 0))
    if idx >= len(years):
        return []

    year, _url = years[idx]
    return _extract_year(page_payload, year)


def main() -> int:
    if html is None:
        print('This example requires lxml. Install it: pip install lxml')
        return 2

    year_from, year_to = (2023, 2026)
    n_years = year_to - year_from + 1
    expected_pages = 1 + n_years

    schema = {
        'base_url': ORIGIN,
        'state': {
            'year_from': year_from,
            'year_to': year_to,
        },
        'endpoints': {
            'pcal': {
                'path': BASE_PATH,
                'method': 'GET',
                'canonical_parser': _canonical_lxml_html,
                # The committed fixture is text-backed for readability;
                # canonical_parser still receives .content as bytes.
                'response_format': 'text',
                'playback': {
                    'mode': 'auto',
                    'path': FIXTURE_PATH,
                },
                'pagination': {
                    'next_request': next_request,
                },
                'on_response': on_response,
                'update_state': update_state,
            }
        },
    }
    client = APIClient(schema)

    out_path = Path('examples/output/pcal_days.csv')
    out_path.parent.mkdir(parents=True, exist_ok=True)

    run = client.stream_run(
        'pcal',
        max_pages=expected_pages + 1,
        max_requests=expected_pages + 1,
        time_limit=max(10.0, expected_pages * 5.0),
    )

    row_count = 0
    with out_path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'month_ru', 'kind', 'working', 'year'])
        writer.writeheader()
        for page_rows in run:
            for row in page_rows:
                writer.writerow(row)
                row_count += 1

    print(f'Wrote {row_count} rows to {out_path.resolve()}')

    if run.summary is None:
        print('No summary (iteration aborted early)')
    else:
        print('Summary:', run.summary)
        stop = getattr(run.summary, 'stop', None)
        if stop is not None:
            print(f'Stop: {stop.kind} ({stop.observed}/{stop.limit})')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
