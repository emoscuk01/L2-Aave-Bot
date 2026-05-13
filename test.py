import requests
API_KEY = "92fae7831d6457b5fa01a5d3e1fdfff2"
r = requests.post(
    f"https://gateway-arbitrum.network.thegraph.com/api/{API_KEY}/subgraphs/id/3RWFxWNstn4nP3dXiDfKi9GgBoHx7xzc7APkXs1MLEgi",
    json={"query": '{ positions(first: 5, where: { side: BORROWER, balance_gt: "0" }) { id account { id } } }'}
)
print(r.json())