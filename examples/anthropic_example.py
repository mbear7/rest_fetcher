'''

live test script for the Anthropic Claude API.
exercises: custom header auth, models list with cursor pagination,
messages endpoint, token counting.

run from the directory containing rest_fetcher/:
    python anthropic_example.py

requires an Anthropic API key — get one at https://platform.claude.com/settings/keys
'''

if __name__ == '__main__':

    import json

    from rest_fetcher import APIClient

    ANTHROPIC_API_KEY = 'your-api-key-here'
    MODEL = 'claude-haiku-4-5-20251001'   # cheapest model for testing


    def pprint(label, data):
        print(f'\n--- {label} ---')
        print(json.dumps(data, indent=2, default=str))


    # anthropic uses two required headers for auth — cleanest via callback
    def anthropic_auth(req, config):
        return {
            **req,
            'headers': {
                **req.get('headers', {}),
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
            }
        }


    # models list uses has_more + last_id cursor pagination
    # GET /v1/models?limit=5&after_id=<last_model_id>
    # response: { data: [...], has_more: bool, first_id: str, last_id: str }
    def models_next_request(parsed_body, state):
        if not parsed_body.get('has_more'):
            return None
        return {'params': {'after_id': parsed_body['last_id']}}


    client = APIClient({
        'base_url': 'https://api.anthropic.com',
        'auth': {'type': 'callback', 'handler': anthropic_auth},
        'timeout': 30,
        'retry': {
            'max_attempts': 3,
            'backoff': 'exponential',
            'on_codes': [429, 500, 502, 503],
        },
        'rate_limit': {
            'respect_retry_after': True,
            'min_delay': 0.5,
        },
        'log_level': 'medium',
        'endpoints': {

            # list all available models — paginated with has_more + last_id cursor
            'models': {
                'method': 'GET',
                'path': '/v1/models',
                'params': {'limit': 5},      # small page size to exercise pagination
                'pagination': {
                    'next_request': models_next_request,
                },
                'on_response': lambda resp, state: resp.get('data', []),
                'on_complete': lambda pages, state: [m for page in pages for m in page],
            },

            # send a message — single response, extract text content
            'messages': {
                'method': 'POST',
                'path': '/v1/messages',
                'on_response': lambda resp, state: {
                    'text': resp['content'][0]['text'],
                    'model': resp['model'],
                    'usage': resp['usage'],
                    'stop_reason': resp['stop_reason'],
                },
            },

            # count tokens before sending — useful for cost estimation
            # requires beta header: token-counting-2024-11-01
            'count_tokens': {
                'method': 'POST',
                'path': '/v1/messages/count_tokens',
                'headers': {'anthropic-beta': 'token-counting-2024-11-01'},
            },
        }
    })

    print('\n' + '=' * 55)
    print('  Anthropic Claude API — rest_fetcher live test')
    print('=' * 55)

    # 1. list all models (paginated, 5 per page)
    print('\n--- all available models (paginated, 5 per page) ---')
    models = client.fetch('models')
    print(f'total models returned: {len(models)}')
    for m in models:
        print(f"  {m['id']:45s}  created: {m['created_at'][:10]}")

    # 2. count tokens before sending
    prompt = 'What is the capital of Denmark? Answer in one sentence.'
    token_count = client.fetch('count_tokens', body={
        'model': MODEL,
        'messages': [{'role': 'user', 'content': prompt}],
    })
    pprint(f'token count for prompt: "{prompt}"', token_count)

    # 3. send a message
    result = client.fetch('messages', body={
        'model': MODEL,
        'max_tokens': 64,
        'messages': [{'role': 'user', 'content': prompt}],
    })
    pprint('message response', result)

    print('\n' + '=' * 55)
    print('  all done')
    print('=' * 55 + '\n')
