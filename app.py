from kafka import KafkaProducer
from flask import Flask, request, jsonify
import json

app = Flask(__name__)
producer = KafkaProducer(
    bootstrap_servers='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/order', methods=['POST'])
def create_order():
    order = request.json
    producer.send('orders', value=order)
    return jsonify({"status": "Order placed!"}), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)