'''
Extract: mengambil harga crypto terbaru (USD) dari API publik CoinGecko
lalu menyimpannya ke CSV untuk diproses tahap transform.
'''
import os

import pandas as pd
import requests

URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = ["bitcoin", "ethereum", "ripple", "solana", "dogecoin"]
OUTPUT = "/workspace/data/crypto/extract_result.csv"


def extract_prices(coins):
    '''
    Mengambil harga beberapa coin dalam USD.

    Parameters:
        coins: list of str - id coin di CoinGecko

    Return:
        DataFrame pandas dengan kolom coin_name, coin_price
    '''
    response = requests.get(URL, params={"ids": ",".join(coins), "vs_currencies": "usd"}, timeout=30)
    response.raise_for_status()  # gagal -> task error -> Airflow retry
    data = response.json()
    return pd.DataFrame({"coin_name": list(data), "coin_price": [v["usd"] for v in data.values()]})


if __name__ == "__main__":
    df = extract_prices(COINS)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    df.to_csv(OUTPUT, index=False)
    print(df)
    print(f"Tersimpan: {OUTPUT}")
