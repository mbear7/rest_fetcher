from __future__ import annotations

import sys
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parents[1]
except NameError:  # notebook / REPL
    ROOT = Path.cwd().resolve()
    for _ in range(6):
        if (ROOT / 'rest_fetcher').is_dir():
            break
        ROOT = ROOT.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from typing import Any

from rest_fetcher import APIClient


def main() -> None:
    events: list[tuple[str, str, Any]] = []

    # Fake monotonic clock + sleeper so the example is fast and deterministic.
    now = {'t': 0.0}

    def clock() -> float:
        return now['t']

    def sleep(seconds: float) -> None:
        now['t'] += seconds

    def on_event(ev) -> None:
        events.append((ev.kind, ev.source, ev.data))

    client = APIClient(
        {
            'base_url': 'https://example.invalid',
            'on_event': on_event,
            'rate_limit': {
                'strategy': 'token_bucket',
                'requests_per_second': 1.0,  # sustained refill rate
                'burst': 1,  # bucket capacity / max immediate spike
                'on_limit': 'wait',
                'clock': clock,
                'sleep': sleep,
            },
            'endpoints': {
                't': {
                    'method': 'GET',
                    'path': '/t',
                    'response_format': 'json',
                    # Mock is treated as a live run (events use source='live'), but it avoids real HTTP.
                    'mock': [
                        {'status_code': 200, 'body': {'items': [1]}},
                        {'status_code': 200, 'body': {'items': [2]}},
                    ],
                    'pagination': {
                        'next_request': lambda parsed_body, state: None
                        if state.get('n', 0) >= 2
                        else {'url': 'https://example.invalid/t?page=2'},
                    },
                    'update_state': lambda page_payload, state: {'n': state.get('n', 0) + 1},
                }
            },
        }
    )

    pages = client.fetch('t')
    print('pages:', pages)

    # A tiny summary
    live = [e for e in events if e[1] == 'live']
    kinds = [k for k, _, _ in live]
    print('event kinds (live):', kinds)
    waited = [d for k, _, d in live if k == 'rate_limit_wait_end']
    if waited:
        print('rate-limited wait_ms:', waited[-1].get('wait_ms'))

    print('fake clock seconds:', now['t'])


if __name__ == '__main__':
    main()
