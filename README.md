# 🪙 BTC and ETH Price Data

This repository contains complete CSV files with candlestick data for Bitcoin (BTC) and Ethereum (ETH) price data, including various timeframes.

## 📊 Data Sources

### Bitcoin (BTC)

- **Bitstamp**: 11-08-2011 - 21-07-2015
- **Coinbase**: 21-07-2015 - 24-09-2025

### Ethereum (ETH)

- **Coinbase**: 24-05-2016 - 24-09-2025

## 📁 Data Location

The CSV files are located in the `/data/` folder, organized by cryptocurrency:

- `/data/btc/` for Bitcoin data
- `/data/eth/` for Ethereum data

### Bitcoin (BTC) Data Structure

- **1min/**: 📈 1-minute candlestick data per year (2011-2025)
- **5min/**: 📊 5-minute aggregated candlesticks per year (2011-2025) and combined full file
- **30min/**: 📊 30-minute aggregated candlesticks (full period)
- **hourly/**: 📊 Hourly aggregated candlesticks (full period)
- **daily/**: 📊 Daily aggregated candlesticks (full period)
- **weekly/**: 📊 Weekly aggregated candlesticks (full period)

Each candlestick file contains columns: `timestamp`, `open`, `close`, `volume`, `unix_timestamp`, `high`, `low`

### Ethereum (ETH) Data Structure

- 📈 1-minute candlestick data per year (2016-2025)

Each file contains 1-minute candlestick data for the respective year.
