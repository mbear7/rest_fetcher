"""

BoozeAPI — pagination test using rest_fetcher.
https://boozeapi.com/api/v1

Response shape:
    { "pagination": {"count": 629, "pages": 210}, "data": [...] }

Pagination: page= + limit= query params, page_number style.
next_request uses pagination.pages from response.

run from the directory containing rest_fetcher/:
    python booze_example.py
"""

if __name__ == '__main__':
    import json

    from rest_fetcher import APIClient

    def pprint(label, data):
        print(f'\n--- {label} ---')
        print(json.dumps(data, indent=2, default=str))

    def flatten_with_stop(pages, state):
        stop = state.get('stop')
        if stop:
            print(f'  stopped by {stop["kind"]} at {stop["observed"]} / {stop["limit"]}')
        return [item for page in pages for item in page]

    def booze_next_request(parsed_body, state):
        "stop when current page >= total pages; caller applies page cap via max_pages="
        # read current page from _request.params (what was actually sent this page)
        # instead of state['page'] = next_page, which would raise StateViewMutationError
        req_params = (state.get('_request') or {}).get('params') or {}
        current = req_params.get('page', 1)
        total = parsed_body.get('pagination', {}).get('pages', 1)
        if current >= total:
            return None
        return {'params': {'page': current + 1}}

    client = APIClient(
        {
            'base_url': 'https://boozeapi.com/api/v1',
            'timeout': 15,
            'log_level': 'medium',
            'endpoints': {
                'cocktails': {
                    'method': 'GET',
                    'path': '/cocktails/',
                    'params': {'limit': 10},
                    'pagination': {
                        'next_request': booze_next_request,
                        'delay': 0.2,
                    },
                    'on_response': lambda resp, state: resp.get('data', []),
                    'on_complete': flatten_with_stop,
                },
                'ingredients': {
                    'method': 'GET',
                    'path': '/ingredients/',
                    'params': {'limit': 10},
                    'pagination': {
                        'next_request': booze_next_request,
                        'delay': 0.2,
                    },
                    'on_response': lambda resp, state: resp.get('data', []),
                    'on_complete': flatten_with_stop,
                },
                # single cocktail by id — no pagination
                'cocktail': {
                    'method': 'GET',
                    'path': '/cocktails/{id}',
                },
            },
        }
    )

    print('\n' + '=' * 55)
    print('  BoozeAPI — rest_fetcher pagination test')
    print('=' * 55)

    # 1. fetch cocktails — max_pages=3 as a polite cap on the free public API.
    # page/request caps are non-destructive: fetch() returns the bounded result normally.
    # if you care why it stopped, inspect state['stop'] inside on_complete.
    print('\nfetching cocktails (limit=10, max 3 pages)...')
    cocktails = client.fetch('cocktails', max_pages=3)
    print(f'total fetched: {len(cocktails)}')
    print(f'first: {cocktails[0]["name"]}')
    print(f'last:  {cocktails[-1]["name"]}')

    # 2. fetch ingredients — same cap, same normal return behavior
    print('\nfetching ingredients (limit=10, max 3 pages)...')
    ingredients = client.fetch('ingredients', max_pages=3)
    print(f'total fetched: {len(ingredients)}')
    print(f'first: {ingredients[0]["name"]}')
    print(f'last:  {ingredients[-1]["name"]}')

    # 3. stream cocktails page by page — no on_complete, so this stays the simplest
    # incremental form: one page arrives, we handle it, then move on. max_pages=5 on
    # stream(): yields completed pages, then stops cleanly.
    print('\nstreaming cocktails page by page (limit=10, max 5 pages)...')
    stream_client = APIClient(
        {
            'base_url': 'https://boozeapi.com/api/v1',
            'timeout': 15,
            'log_level': 'medium',
            'endpoints': {
                'cocktails': {
                    'method': 'GET',
                    'path': '/cocktails/',
                    'params': {'limit': 10},
                    'pagination': {
                        'next_request': booze_next_request,
                        'delay': 0.1,
                    },
                    'on_response': lambda resp, state: resp.get('data', []),
                },
            },
        }
    )
    page_count = 0
    item_count = 0
    for page in stream_client.stream('cocktails', max_pages=5):
        page_count += 1
        item_count += len(page)
        print(f'  page {page_count:3d} — {len(page)} items  (running total: {item_count})')
    print(f'  ... stopped after {page_count} pages (limit=5) — stream() preserves partial pages')

    # if you want destructive stop behaviour, time_limit is the one that raises.
    # page/request caps are just local guardrails.

    # 4. single cocktail lookup — use a real id from the cocktails we already fetched
    if cocktails:
        first_id = cocktails[0]['id']
        print(f'\nfetching single cocktail by id ({first_id})...')
        pprint(f'cocktail id={first_id}', client.fetch('cocktail', path_params={'id': first_id}))

    print('\n' + '=' * 55)
    print('  all done')
    print('=' * 55 + '\n')
