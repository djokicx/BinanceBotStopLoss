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
    await socket(client, pair)
    await client.close_connection()


async def socket(client, pair):
    engineName = f"sqlite:///{pair}stream.db"
    engine = sqlalchemy.create_engine(engineName)
    bsm = BinanceSocketManager(client)
    socket = bsm.trade_socket(pair)
    while True:
        await socket.__aenter__()
        msg = await socket.recv()
        # dictionary => frame
        try:
            frame = await createframe(msg)
            frame.to_sql(pair, engine, if_exists="append", index=False)
        except:
            print("Invalid input")
        # print(frame)


async def createframe(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ["s", "E", "p"]]
    df.columns = ["Symbol", "Time", "Price"]
    # df.Price = pd.to_numeric(df["Price"])
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit="ms")
    return df


if __name__ == "__main__":
    asyncio.run(main())
