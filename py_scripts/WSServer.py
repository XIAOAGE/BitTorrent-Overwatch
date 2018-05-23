import asyncio
import websockets
import pymongo
import pprint

FREQUENCY = 5
pipeline = [
  {
    "$group": {
      "_id": "$country",
      "count": {"$sum": 1}
    }
  },
  {
    "$sort": {
      "count": -1
    }
  }
]

class WSServer():
  
  def __init__(self, host='localhost', port=27017):
    client = pymongo.MongoClient(host, port)
    db = client.bt_overwatch_db
    self.coll = db.collection1
    self.cycle_cnt = 1

  async def handler(self, websocket, path):
    while True:
      print("Running through the update batch: ", self.cycle_cnt, websocket)
      self.cycle_cnt += 1
      data = list(self.coll.aggregate(pipeline))
      for x in data:
        await websocket.send(str(x))
      await websocket.send("END")
      await asyncio.sleep(FREQUENCY)

if __name__ == "__main__":  
  wss = WSServer()
  loop = asyncio.get_event_loop()
  start_server = websockets.serve(wss.handler, 'localhost', 8765)
  loop.run_until_complete(start_server)
  loop.run_forever()
    