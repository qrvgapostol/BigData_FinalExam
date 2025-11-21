# Streaming Data Dashboard

This project shows live Bitcoin price data using Kafka, MongoDB, and Streamlit.  
We collect the BTC price from an API, send it into Kafka, save it in MongoDB, and display everything on a dashboard.

## How to run the project

1. Start Docker (Kafka, Zookeeper, and MongoDB):
   docker compose up -d

2. Start the producer to stream live Bitcoin data:
   python producer.py

3. Start the Streamlit dashboard:
   streamlit run app.py

The dashboard will open at:
http://localhost:8501

## Features

### Real time view
- Shows the live Bitcoin price (around $91k)
- Updates every 15 seconds
- Shows change and percent change from the last update
- Table of latest messages from Kafka
- Real time price trend chart

### Historical view
- Choose time ranges (1h, 24h, 7d, 30d)
- Shows minimum, maximum, and average price in the range
- Full table of values from MongoDB
- Trend chart for price
- Trend chart for percent change
- Download historical data as CSV

## Files

- producer.py  
  Streams Bitcoin price data into a Kafka topic.

- app.py  
  Streamlit dashboard with real time and historical views.

- docker-compose.yml  
  Starts Kafka, Zookeeper, and MongoDB using Docker.

## Tools Used

- Python
- Apache Kafka
- MongoDB
- Streamlit
- Docker
- Binance API (BTC price)
