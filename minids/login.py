# 'pip install globus_sdk'
import globus_sdk

# Setup your client at http://developers.globus.org You may use the following
# id for testing.
CLIENT_ID = '795b3536-ad58-4dd5-96f8-499922258a60'

# Requested Scope from the Identifier Client
REQUESTED_SCOPES = ['https://auth.globus.org/scopes/identifiers.globus.org/create_update']
REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'

client = globus_sdk.NativeAppAuthClient(client_id=CLIENT_ID)
# pass refresh_tokens=True to request refresh tokens
client.oauth2_start_flow(requested_scopes=REQUESTED_SCOPES,
                         redirect_uri=REDIRECT_URI,
                         refresh_tokens=True)

url = client.oauth2_get_authorize_url()

print('Native App Authorization URL: \n{}'.format(url))

# Support both python2 and python3
get_input = getattr(__builtins__, 'raw_input', input)

auth_code = get_input('Enter the auth code: ').strip()

token_response = client.oauth2_exchange_code_for_tokens(auth_code)
tokens = token_response.by_resource_server

print('My Identifier Globus Token is {}'.format(tokens['identifiers.globus.org']['access_token']))