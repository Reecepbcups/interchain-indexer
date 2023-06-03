import json
import os
from typing import List, Tuple

# pip install matplotlib
import matplotlib.pyplot as plt
import numpy as np

date_to_price: dict[str, float] = {}

# all_txs_per_day.json is from all_txs_per_day.py
# juno0-usd-max.csv is from coingecko export

current_dir = os.path.dirname(os.path.realpath(__file__))


def coingecko_convert():
    global date_to_price

    file_path = os.path.join(current_dir, "juno-usd-max.csv")
    with open(file_path, "r") as f:
        lines = f.readlines()

    # snapped_at,price,market_cap,total_volume
    for line in lines[1::]:
        snaped_at, price, _, _ = line.replace("\n", "").split(",")
        snaped_at = snaped_at.replace(" 00:00:00 UTC", "")

        date_to_price[snaped_at] = round(float(price), 4)


def get_txs_per_day() -> dict[str, int]:
    with open("all_txs_per_day.json", "r") as f:
        txs_per_day = json.load(f)

    return txs_per_day


def get_all_data_format() -> List[Tuple[str, float, int]]:
    data = []

    txs_per_day = get_txs_per_day()

    # iter date_to_price and txs_per_day
    for date, price in date_to_price.items():
        if date in txs_per_day:
            data.append((date, price, txs_per_day[date]))
        else:
            data.append((date, price, 0))

    return data


def main():
    coingecko_convert()
    # print(date_to_price)

    # Get txs per day
    data = get_all_data_format()

    # print(data[1][1], data[1][2])

    y_values = [int(item[2]) for item in data]

    # x_values = [item[0] for item in data]
    # plt.plot(x_values, y_values, color="black")
    # plt.xlabel("Date")

    x_values = [float(item[1]) for item in data]
    plt.scatter(x_values, y_values)
    plt.xlabel("Token Price")

    # the bigger y_values is, the bigger the bubble relative to the others
    # bubbles_size = [item * 0.1 for item in y_values]
    # plt.scatter(x_values, y_values, s=bubbles_size, alpha=0.25, c="black")

    plt.ylabel("Txs per day")
    plt.title("Chart 'o Juno")

    # Displaying the chart
    plt.show()


if __name__ == "__main__":
    main()
