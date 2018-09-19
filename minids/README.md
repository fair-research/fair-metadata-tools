# Creating Minids

Minids are now created using the Globus Identifier Service. The corresponding
Globus Identifier Client can be used to create Minids, even though it supports
many extra futures not used by minid.

## Using the Globus Identifier Client

### Installation

The CLI is not currently public, please ask Argon to grant you permission for
usage. The client can be found [here](https://github.com/globusonline/globus-identifier-client)

** The CLI requires Python 3 **

If you have ssh setup, you can run the following:

    pip install -U -e git+git@github.com:globusonline/globus-identifier-client.git#egg=globus-identifier-client

### Usage

Installing the CLI grants you the `globus-identifier-client` command, which should
now work in the python environment you installed it in. Begin by running the following
to retrieve tokens, which will be used for future commands:

    globus-identifier-client login

You should now be logged in and be able to create Identifiers. At minimum, you need
to specify the minid namespace and the access level. For this example, we're using the
Minid test namespace `HHxPIZaVDh9u` and making it visible to anyone.

    globus-identifier-client identifier-create --namespace HHxPIZaVDh9u --visible-to '["public"]'

That's it! You can now visit `https://identifiers.globus.org/<your minid>` to see your minid.
Notice the `--visible-to` field is given in JSON. Most fields, including `location` and `metadata`
are specified this way. For example:

    globus-identifier-client identifier-create --namespace HHxPIZaVDh9u --visible-to '["public"]' --metadata '{"foo": "bar"}' --location '["example.com/myfile"]'

### Namespaces

There are two namespaces for Minids in the Globus Identifier Service

* Test Minid Namespace: HHxPIZaVDh9u
* Production Minid Namespace: kHAAfCby2zdn

You can mint in either one, but please only use the Production Minids if you are sure you are going
to use them! Please use the test namespace for testing your scripts!

## Calling into the Python SDK

It's also possible to call into the Identifier Client SDK if you are writing a python script. It
is built on and functions very much like the Globus SDK.

    # The IdentifierClient class is used for all operations
    from identifier_client.identifier_api import IdentifierClient
    # You must specify the `base_url`.
    # If you are unfamiliar with Globus Tokens, see login.py
    ic = IdentifierClient('Identifier',
                      base_url='https://identifiers.globus.org/',
                      app_name='My Local App',
                      authorizer=AccessTokenAuthorizer(token)
                      )

    my_kwargs = {'visible_to': json.dumps(['public'])}
    minid = ic.create_identifier(namespace=HHxPIZaVDh9u,
                                 **my_kwargs)
    print(minid.data)
