import os
import pandas as pd
import sqlalchemy
import asyncio
from binance import BinanceSocketManager, AsyncClient
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

pair = "ADABUSD"


async def main():
    client = await AsyncClient.create(API_KEY, API_SECRET)
    await strategy(client, pair, 0.001, 60, 35)
    await client.close_connection()


async def strategy(client, pair, entry, lookback, qty, open_position=False):
    engineName = f"sqlite:///{pair}stream.db"
    engine = sqlalchemy.create_engine(engineName)

    while True:
        # constantly requesting data from our database that's getting fed
        df = pd.read_sql(pair, engine)
        # from lookback till the end of entries
        lookbackperiod = df.iloc[-lookback:]
        # cummulating returns over the lookback period
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1

        if cumret[cumret.last_valid_index()] < entry:
            order = await client.create_order(
                symbol=pair, side="BUY", type="MARKET", quantity=qty
            )

            print(order)
            open_position = True
            break

        # Trailing Stop Loss

    if open_position:
        while True:
            df = pd.read_sql(
                f"""SELECT * FROM {pair} WHERE Time >= '{pd.to_datetime(order['transactTime'], unit='ms')})'""",
                engine,
            )
            df["Benchmark"] = df.Price.cummax()
            # Arbitrary chosen...0.5% is likely to small and will be triggered easily. Just for testing
            df["TSL"] = df.Benchmark * 0.995
            if df[df.Price < df.TSL].first_valid_index():
                order = await client.create_order(
                    symbol=pair, side="SELL", type="MARKET", quantity=qty
                )
                print(order)
                break


if __name__ == "__main__":
    asyncio.run(main())
