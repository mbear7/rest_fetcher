"""GLPI v2 example using oauth2_password auth.

This example is intentionally conservative about endpoint specifics. The auth
flow is grounded in GLPI's v2 docs; endpoint paths, filter support, and
pagination details should be confirmed against your own instance's
/api.php/doc or /api.php/doc.json when that documentation is enabled.
"""

from rest_fetcher import APIClient
from rest_fetcher import SchemaBuilder

client = APIClient({
    'base_url': 'https://glpi.example.com/api.php/v2',
    'auth': {
        'type': 'oauth2_password',
        'token_url': 'https://glpi.example.com/api.php/token',
        'client_id': 'my-client-id',
        'client_secret': 'my-client-secret',
        'username': 'glpi-api-user',
        'password': 'correct horse battery staple',
        'scope': 'api',
    },
    'endpoints': {
        # Single-item reads are a safe starting point when you do not yet have
        # your instance's OpenAPI doc handy.
        'ticket': {
            'method': 'GET',
            'path': '/Ticket/{ticket_id}',
        },
    }
})

# Read one ticket. path_params fills the {ticket_id} placeholder.
ticket = client.fetch('ticket', path_params={'ticket_id': 123})
print(ticket)


# SchemaBuilder version
schema = (
    SchemaBuilder('https://glpi.example.com/api.php/v2')
    .oauth2_password(
        token_url='https://glpi.example.com/api.php/token',
        client_id='my-client-id',
        client_secret='my-client-secret',
        username='glpi-api-user',
        password='correct horse battery staple',
        scope='api',
    )
    .endpoint('ticket', method='GET', path='/Ticket/{ticket_id}')
    .build()
)

# Optional: if your instance docs show collection endpoints that support RSQL
# filtering, you can call them like this:
#
# tickets = APIClient({
#     **schema,
#     'endpoints': {
#         'tickets': {
#             'method': 'GET',
#             'path': '/Ticket',
#             'params': {'filter': 'status="new"'},
#         }
#     }
# }).fetch('tickets')
