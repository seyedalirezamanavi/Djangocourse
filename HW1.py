import json
import websocket
import numpy as np
from sqlalchemy import Boolean, Column, Float, ForeignKey, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker



class Market:

    def __init__(self,market_name):
        self.market_name = market_name
    
    


class Pair(Market):

    def __init__(self,market_name,crypto):
        super().__init__(market_name)
        self.crypto = crypto
    def make_req(self):
        return(self.crypto+'-'+self.market_name)



############ database ###############

market = "coinbase"
engine = create_engine('sqlite:///'+market+'.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()
class Trade(Base):
    __tablename__ = 'trade'
    id = Column(Integer, primary_key=True)
    time_stamp = Column(String)
    price = Column(Float)
    amount = Column(Float)
    position = Column(String)
    def __init__(self, time_stamp, amount, position,price):
        self.time_stamp = time_stamp
        self.price = price
        self.amount = amount
        self.position = position
Base.metadata.create_all(engine)


request_string = Pair("USD","BTC").make_req()

req = {"type": "subscribe","product_ids": [request_string],"channels": ["level2"]}
url = "wss://ws-feed.pro.coinbase.com"

################# websocket ##################

ws = websocket.create_connection(url)
print("connection has made")
ws.send(json.dumps(req))
print("the request for server has sent")
j=0
while True:
    try:
        j+=1
        result = ws.recv()
        result = json.loads(result)
        if j>2:
            for data in result["changes"]:    
                session = Session()
                db = Trade(result["time"],data[2],data[0],data[1])
                session.add(db)
                print ("Received '%s | %s | %s | %s '" %(result["time"],data[2],data[0],data[1]))
            session.commit()
    except:
        print("Something bad happend!")
        print("Reconnecting...")
        ws = websocket.create_connection(url)
        print("connection has made")
        ws.send(json.dumps(req))
        print("the request for server has sent")
ws.close()



