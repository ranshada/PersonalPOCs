from coindesk.client import CoinDeskAPIClient
api_client = CoindeskAPIClient.start('currentprice')
response = api_client.get(raw=True)