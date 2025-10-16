# Real-Time Stock Market Anomaly Detector

This application is a real-time stock market analysis tool that combines a squeeze scanner with an anomaly detector. It identifies stocks that are in a "squeeze" (a period of low volatility that often precedes a significant price movement) and then monitors them for unusual trading activity, such as sudden spikes in volume or price.

## Project Structure

The application has been refactored into a modular architecture to improve maintainability and scalability. Here's an overview of the project structure:

-   `config/`: Contains the centralized configuration for the application.
-   `data/`: Stores data files, such as `instruments.csv` and the SQLite database.
-   `scanner/`: Includes the logic for the squeeze scanner.
-   `utils/`: Contains helper functions for mapping, market hours, and data storage.
-   `web/`: Holds the Flask web application and its templates.
-   `wss/`: Contains the WebSocket client for real-time data fetching and anomaly detection.
-   `main.py`: The main entry point for the application.

## Setup Guide

Follow these steps to set up and run the application:

### 1. Install Dependencies

Install all the required Python packages using the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

### 2. Configure the Application

Edit the `config/config.py` file to set up your configuration:

-   **`ACCESS_TOKEN`**: Your Upstox API access token.
-   **`MONGO_URI`**: The connection string for your MongoDB instance.
-   Other settings, such as scanner parameters and file paths, can also be adjusted in this file.

### 3. Prepare the Data

-   Make sure you have an `instruments.csv` file in the `data/` directory. This file should contain the instrument details for the stocks you want to track.
-   The application will automatically create a `squeeze_history.db` SQLite database in the `data/` directory.

## How to Run the Application

Once you have completed the setup, you can run the application using the `main.py` script:

```bash
python main.py
```

This will start the following components:

-   **Background Scanner**: A thread that periodically scans for stocks in a squeeze.
-   **Flask Web Server**: A web interface to view the scanner results and anomalies, accessible at `http://127.0.0.1:5001/`.
-   **WebSocket Client**: A client that connects to the Upstox WebSocket API to fetch real-time data and detect anomalies.