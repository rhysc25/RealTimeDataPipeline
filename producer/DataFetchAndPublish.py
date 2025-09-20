import websocket
from APIKeys import APIKey
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='kafka:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # convert to bytes for kafka topic
)

def kafkaPublish(x):
    producer.send('market-ticks', x)
    producer.flush()

def on_message(ws, message):
    try: 
        payload = json.loads(message)    
        data = payload.get("data", [])         
        for x in data:                         
            kafkaPublish(x)      
    except: pass

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={APIKey}",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()