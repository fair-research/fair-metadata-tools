import globus_sdk
import json

CLIENT_ID = '61f6ddd6-ef14-4a3b-b6fa-cfbdbc2e00de'
CONCIERGE_SCOPE_NAME = '524361f2-e4a9-4bd0-a3a6-03e365cac8a9'
SCOPES = ['https://auth.globus.org/scopes/'
          '524361f2-e4a9-4bd0-a3a6-03e365cac8a9/concierge',
          'urn:globus:auth:scope:search.api.globus.org:all']
TEMP_CREDS = 'search_ingest_tokens.json'


def load_tokens():
    try:
        with open(TEMP_CREDS) as f:
            tokens = json.loads(f.read())
    except FileNotFoundError:
        client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
        client.oauth2_start_flow(requested_scopes=SCOPES)
        authorize_url = client.oauth2_get_authorize_url()
        print('Visit the URL below: \n{}'.format(authorize_url))
        auth_code = input(
          'Please enter the code you get after login here: ').strip()
        token_response = client.oauth2_exchange_code_for_tokens(auth_code)
        tokens = token_response.by_resource_server
        with open(TEMP_CREDS, 'w+') as f:
            f.write(json.dumps(tokens))
        print('Saved creds to "{}" for future ingests.'.format(TEMP_CREDS))
    return tokens


def load_search_client():
    tokens = load_tokens()
    auth = globus_sdk.AccessTokenAuthorizer(tokens['search.api.globus.org']
                                            ['access_token'])
    return globus_sdk.SearchClient(authorizer=auth)