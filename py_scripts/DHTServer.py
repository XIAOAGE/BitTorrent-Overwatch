import socket
import GeoIP

from random import randint
from struct import unpack
from hashlib import sha1
from collections import deque
from time import sleep
from threading import Timer, Thread
from bencode import bencode
from pykafka import KafkaClient


BOOTSTRAPING_NODES = [
  ("router.bittorrent.com", 6881),
  ("dht.transmissionbt.com", 6881),
  ("router.utorrent.com", 6881)
]

TXID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2

def entropy(length):
  return "".join(chr(randint(0, 255)) for _ in range(length))

def random_id():
  h = sha1()
  h.update(entropy(20).encode('utf-8'))
  return h.digest()

def decode_nodes(nodes):
  ret = []
  length = len(nodes)
  if (length % 26) != 0: return ret

  for i in range(0, length, 26):
    nid = nodes[i:i+20]
    ip = socket.inet_ntoa(nodes[i+20:i+24])
    port = unpack("!H", nodes[i+24:i+26])[0]
    ret.append((nid, ip, port))
  
  return ret

def get_neighbour(target, nid, length=10):
  return target[:length] + nid[length:]

class KNode(object):
  
  def __init__(self, nid, ip, port):
    self.nid = nid
    self.ip = ip
    self.port = port
  
class DHTServer(Thread):

  def __init__(self, bind_ip, bind_port, max_qsize):
    Thread.__init__(self)
    # self.setDaemon(True)

    self.bind_ip = bind_ip
    self.bind_port = bind_port
    self.max_qsize = max_qsize
    self.nid = random_id()
    self.nodes = deque(maxlen=max_qsize)
    self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    self.ufd.bind((self.bind_ip, self.bind_port))
    self.client = KafkaClient('localhost:9092')
    self.topic = self.client.topics[b'ip_address']
    self.producer = self.topic.get_producer()
    self.gi = GeoIP.new(GeoIP.GEOIP_MEMORY_CACHE)
  
    self.process_request_actions = {
      "get_peers": self.on_get_peers_request,
      "announce_peer": self.on_announce_peer_request
    }
    Timer(RE_JOIN_DHT_INTERVAL, self.rejoin_DHT)
  
  def run(self):
    self.rejoin_DHT()
    while True:
      try:
        (data, address) = self.ufd.recvfrom(65536)
        msg = bencode.decode(data)
        self.on_message(msg, address)
      except Exception:
        pass
  
  def send_krpc(self, msg, address):
    try:
      self.ufd.sendto(bencode.encode(msg), address)
    except Exception as e:
      print("WRONG!")
      pass

  def send_find_node(self, address, nid=None):
    fake_nid = get_neighbour(nid, self.nid) if nid else self.nid
    txid = entropy(TXID_LENGTH)
    msg = {
      "t": txid,
      "y": "q",
      "q": "find_node",
      "a": {
        "id": fake_nid,
        "target": random_id()
      }
    }
    self.send_krpc(msg, address)

  def join_DHT(self):
    for address in BOOTSTRAPING_NODES:
      self.send_find_node(address)
  
  def rejoin_DHT(self):
    if len(self.nodes) == 0: 
      #print("Reaching out to the bootstraping nodes...")
      self.join_DHT()
    Timer(RE_JOIN_DHT_INTERVAL, self.rejoin_DHT).start()
  
  def auto_send_find_node(self):
    wait = 1.0 / self.max_qsize
    while True:
      try:
        node = self.nodes.popleft()
        #print("Checking out the neighbour "+str(node.ip)+":"+str(node.port))
        self.send_find_node((node.ip, node.port), node.nid)
      except IndexError:
        pass
      sleep(wait)
  
  def on_message(self, msg, address):
    try:
      if msg["y"] == "r":
        if "nodes" in msg["r"]:
          self.process_find_node_response(msg)
      elif msg["y"] == "q":
        try:
          self.process_request_actions[msg["q"]](msg, address)
        except KeyError:
          #print("on other request")
          self.play_dead(msg, address)
    except KeyError:
      pass

  def process_find_node_response(self, msg):
    nodes = decode_nodes(msg["r"]["nodes"])
    for node in nodes:
      (nid, ip, port) = node
      if len(nid) != 20: continue
      if ip == self.bind_ip: continue
      if port < 1 or port > 65535: continue
      n = KNode(nid, ip, port)
      self.nodes.append(n)
      #print(ip)
    
  def on_get_peers_request(self, msg, address):
    try:
      infohash = msg["a"]["info_hash"]
      txid = msg["t"]
      token = infohash[:TOKEN_LENGTH]
      reply = {
        "t": txid,
        "y": "r",
        "r": {
          "id": get_neighbour(infohash, self.nid),
          "nodes": "",
          "token": token
        }
      }
      kafka_msg = "mongo ip:"+address[0]+"|country:"+self.gi.country_name_by_addr(address[0])
      print(kafka_msg)
      self.producer.produce(kafka_msg.encode('utf-8'))
      self.send_krpc(reply, address)
    except KeyError:
      pass

  def on_announce_peer_request(self, msg, address):
    try:
      infohash = msg["a"]["info_hash"]
      token = msg["a"]["token"]
      nid = msg["a"]["id"]
      txid = msg["t"]
      if infohash[:TOKEN_LENGTH] == token:
        if "implied_port" in msg["a"] and msg["a"]["implied_port"] != 0:
            port = address[1]
        else:
            port = msg["a"]["port"]
            if port < 1 or port > 65535: return
        print('on announce_peer: ', address[0])
        #self.producer.produce(self.gi.country_name_by_addr(address[0]).encode('utf-8'))
    except Exception:
      pass
    finally:
      self.ok(msg, address)
        

  def play_dead(self, msg, address):
    try:
      reply = {
        "t": msg["t"],
        "y": "e",
        "e": [202, "Server Error"]
      }
      self.send_krpc(reply, address)
    except KeyError:
      pass
  
  def ok(self, msg, address):
    try:
      reply = {
        "t": msg["t"],
        "y": "r",
        "r": {
          "id": get_neighbour(msg["a"]["id"], self.nid)
        }
      }
      self.send_krpc(reply, address)
    except KeyError:
      pass

if __name__ == "__main__":
  dht = DHTServer("0.0.0.0", 6882, 300)
  dht.start()
  dht.auto_send_find_node()

