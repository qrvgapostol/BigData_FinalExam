# Streaming Data Dashboard

This project demonstrates how to monitor live Bitcoin price data using a combination of Kafka, MongoDB, and Streamlit. First, the current Bitcoin prices are collected from an external API, ensuring that the data is always up to date. The collected data is then sent into a Kafka message broker, which allows for efficient streaming and real-time processing. Afterward, the data is stored in a MongoDB database, providing a reliable and persistent record of historical price information. Finally, all the collected and processed data is displayed on an interactive Streamlit dashboard, giving users a clear and dynamic view of Bitcoin price trends in real time.

## How to run the project

1. Start Docker (Kafka, Zookeeper, and MongoDB):
   docker compose up -d

2. Start the producer to stream live Bitcoin data:
   python producer.py

3. Start the Streamlit dashboard:
   streamlit run streamlit.py

The dashboard will open at:
http://localhost:9093

## Features

### Real time view
- Shows the live Bitcoin price (around $85k)
- Updates every 15 seconds
- Shows change and percent change from the last update
- Table of latest messages from Kafka
- Real time price trend chart

### Historical view
- Shows minimum, maximum, and average price in the range
- Full table of values from MongoDB
- Trend chart for price
- Trend chart for percent change

## Tools Used

- Python
- Apache Kafka
- MongoDB
- Streamlit
- Docker
- Binance API (BTC price)
