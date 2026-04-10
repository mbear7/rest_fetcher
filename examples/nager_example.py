"""

live test script for date.nager.at public holidays api.
no auth required, no rate limits.
run from the directory containing rest_fetcher/:
    python nager_example.py
"""

if __name__ == '__main__':
    import json

    from rest_fetcher import APIClient

    def pprint(label, data):
        print(f'\n--- {label} ---')
        print(json.dumps(data, indent=2, default=str))

    client = APIClient(
        {
            'base_url': 'https://date.nager.at/api/v3',
            'timeout': 15,
            'log_level': 'medium',
            'endpoints': {
                # path_params interpolated at call time
                'holidays': {
                    'method': 'GET',
                    'path': '/PublicHolidays/{year}/{country}',
                },
                # today check — returns 200 (is holiday) or 204 (not a holiday), no body
                'is_today_holiday': {
                    'method': 'GET',
                    'path': '/IsTodayPublicHoliday/{country}',
                    'on_response': lambda resp, state: {
                        'is_holiday': state['_response_headers'].get('_status_code') == 200
                    },
                },
                # upcoming holidays for a country
                'next_holidays': {
                    'method': 'GET',
                    'path': '/NextPublicHolidays/{country}',
                },
                # full list of supported countries — no path params needed
                'countries': {
                    'method': 'GET',
                    'path': '/AvailableCountries',
                },
            },
        }
    )

    print('\n' + '=' * 50)
    print('  date.nager.at — public holidays api test')
    print('=' * 50)

    # 1. all supported countries
    result = client.fetch('countries')
    pprint(f'available countries ({len(result)} total, first 5)', result[:5])

    # 2. holidays for a specific year + country
    result = client.fetch('holidays', path_params={'year': 2026, 'country': 'DK'})
    pprint('public holidays 2026 — Denmark', result)

    # 3. same for US
    result = client.fetch('holidays', path_params={'year': 2026, 'country': 'US'})
    pprint('public holidays 2026 — United States', result)

    # 4. next upcoming holidays for Denmark
    result = client.fetch('next_holidays', path_params={'country': 'DK'})
    pprint('next public holidays — Denmark', result)

    # 5. is today a holiday in Denmark?
    # note: returns 200 with no body if yes, 204 if no — handle both
    result = client.fetch('is_today_holiday', path_params={'country': 'DK'})
    pprint('is today a holiday in Denmark?', result)

    print('\n' + '=' * 50)
    print('  all done')
    print('=' * 50 + '\n')
