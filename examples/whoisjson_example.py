"""

live test script for whoisjson.com api.
run from the directory containing rest_fetcher/:
    python whoisjson_example.py
"""

if __name__ == '__main__':
    import json

    from rest_fetcher import APIClient

    TOKEN = 'token'

    def pprint(data):
        print(json.dumps(data, indent=2, default=str))

    # on_response is NOT inherited from client level — only headers, on_error,
    # log_level, and response_parser propagate to endpoints. use response_parser
    # (2-arg form) for a client-level side-effect that fires on every endpoint.
    def log_remaining(response, parsed):
        hdrs = dict(response.headers)
        print(f'  remaining requests: {hdrs.get("Remaining-Requests", "?")}')
        return parsed  # return unchanged so callers get the normal response dict

    client = APIClient(
        {
            'base_url': 'https://whoisjson.com/api/v1',
            'auth': {
                'type': 'callback',
                'handler': lambda req, config: {
                    **req,
                    'headers': {**req.get('headers', {}), 'Authorization': f'TOKEN={TOKEN}'},
                },
            },
            'retry': {
                'max_attempts': 3,
                'backoff': 'exponential',
                'on_codes': [429, 500, 502, 503],
            },
            'rate_limit': {
                'respect_retry_after': True,
                'min_delay': 3.1,  # 20 req/min on free tier
            },
            'timeout': (10, 60),  # (connect_timeout, read_timeout) — api can be slow
            'log_level': 'medium',
            'response_parser': log_remaining,  # 2-arg: inherited by all endpoints, prints remaining
            'endpoints': {
                'whois': {'method': 'GET', 'path': '/whois'},
                'dns': {'method': 'GET', 'path': '/nslookup'},
                'availability': {'method': 'GET', 'path': '/domain-availability'},
                'ssl': {'method': 'GET', 'path': '/ssl-cert-check'},
                'subdomains': {'method': 'GET', 'path': '/subdomains'},
                'reverse_whois': {'method': 'GET', 'path': '/reverseWhois'},
            },
        }
    )

    domain = 'python.org'

    print(f'\n{"=" * 50}')
    print(f'  whoisjson.com live test — domain: {domain}')
    print(f'{"=" * 50}')

    print('\n--- whois ---')
    pprint(client.fetch('whois', params={'domain': domain}))

    print('\n--- dns ---')
    pprint(client.fetch('dns', params={'domain': domain}))

    print('\n--- ssl ---')
    pprint(client.fetch('ssl', params={'domain': domain}))

    print('\n--- availability ---')
    pprint(client.fetch('availability', params={'domain': domain}))

    print('\n--- reverse whois (registrar ip) ---')
    pprint(client.fetch('reverse_whois', params={'ip': '45.55.99.72'}))

    print(f'\n{"=" * 50}')
    print('  all done')
    print(f'{"=" * 50}\n')
