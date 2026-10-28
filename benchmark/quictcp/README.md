# Performance Evaluation of MQTT-over-QUIC and MQTT-over-TCP
This benchmark focuses on evaluating the advantages of QUIC over TCP across various network conditions.

## Overview

## Testbed Setup
### Broker Configuration
The broker used in this benchmark is `EMQX` (`emqx-enterprise:6.3.1`) which supports both MQTT over QUIC and TCP.

### Client Implementation
- MQTT over QUIC: Implemented using [`mqttOQ`](https://github.com/hphuc15/mqttOQ).
- MQTT over TCP: Implemented using [`paho.mqtt.golang`](https://github.com/eclipse-paho/paho.mqtt.golang).
