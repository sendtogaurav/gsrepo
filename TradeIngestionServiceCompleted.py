"""
TRADE INGESTION SERVICE - SAMPLE IMPLEMENTATION

This is a sample completed version of the TradeIngestionService class
using the producer-consumer pattern. It defaults to simulation mode
for trade generation and includes basic error handling.
"""

import threading
import queue
import time
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum
import random


class TradeStatus(Enum):
    FILLED = "Filled"
    PARTIAL = "Partial"
    CANCELED = "Canceled"


class TradeSide(Enum):
    BUY = "Buy"
    SELL = "Sell"


class DataSource(Enum):
    SIMULATION = "simulation"
    API = "api"
    KAFKA = "kafka"


# Sample Trade Data Structure (DO NOT MODIFY)
class Trade:
    def __init__(self, trade_id: int, symbol: str, side: TradeSide,
                 quantity: int, price: float, timestamp: datetime,
                 status: TradeStatus, source: DataSource):
        self.trade_id = trade_id
        self.symbol = symbol
        self.side = side
        self.quantity = quantity
        self.price = price
        self.timestamp = timestamp
        self.status = status
        self.source = source

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'quantity': self.quantity,
            'price': self.price,
            'timestamp': self.timestamp.isoformat(),
            'status': self.status.value,
            'source': self.source.value
        }


# VALID SYMBOLS (DO NOT MODIFY)
VALID_SYMBOLS = {'AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'}


class TradeIngestionService:
    """
    Real-time Trade Ingestion Service using Producer-Consumer Pattern
    """

    def __init__(self, queue: Optional[queue.Queue] = None):
        self.trade_queue = queue if queue else queue.Queue()
        self.running = threading.Event()
        self.producer_thread = None
        self._lock = threading.Lock()
        self.trade_id_counter = 0
        self.data_source = DataSource.SIMULATION  # Default to simulation

    def start(self):
        """Start the ingestion service"""
        if self.producer_thread is not None:
            return  # Already running
        self.running.set()
        self.producer_thread = threading.Thread(target=self._producer_loop, daemon=True)
        self.producer_thread.start()

    def stop(self):
        """Gracefully stop the service"""
        self.running.clear()
        if self.producer_thread:
            self.producer_thread.join(timeout=2.0)  # Wait up to 2 seconds
            self.producer_thread = None

    def _producer_loop(self):
        """Main producer loop - fetch, validate, enqueue trades"""
        while self.running.is_set():
            try:
                if self.data_source == DataSource.SIMULATION:
                    trade_data = self._generate_sample_trade()
                elif self.data_source == DataSource.API:
                    trade_data = self._fetch_from_api()
                    if trade_data is None:
                        time.sleep(1)  # Wait if no data
                        continue
                else:
                    # Placeholder for other sources like Kafka
                    time.sleep(1)
                    continue

                if self._validate_trade(trade_data):
                    with self._lock:
                        self.trade_id_counter += 1
                        trade_data['trade_id'] = self.trade_id_counter
                    self.trade_queue.put(trade_data)
                else:
                    print("Invalid trade discarded")

                time.sleep(random.uniform(0.5, 2.0))  # Simulate real-time delay
            except Exception as e:
                print(f"Error in producer loop: {e}")
                time.sleep(1)  # Backoff on error

    def _validate_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Validate trade data before enqueuing"""
        if 'quantity' not in trade_data or trade_data['quantity'] <= 0:
            return False
        if 'price' not in trade_data or trade_data['price'] <= 0:
            return False
        if 'symbol' not in trade_data or trade_data['symbol'] not in VALID_SYMBOLS:
            return False
        if 'side' not in trade_data or trade_data['side'] not in [s.value for s in TradeSide]:
            return False
        if 'status' not in trade_data or trade_data['status'] not in [s.value for s in TradeStatus]:
            return False
        if 'timestamp' not in trade_data:
            return False
        if 'source' not in trade_data:
            return False
        return True

    def _generate_sample_trade(self) -> Dict[str, Any]:
        """Generate sample trade for simulation"""
        symbol = random.choice(list(VALID_SYMBOLS))
        side = random.choice(list(TradeSide)).value
        quantity = random.randint(100, 1000)
        price = round(random.uniform(50, 500), 2)
        timestamp = datetime.now().isoformat()
        status = random.choice(list(TradeStatus)).value
        source = DataSource.SIMULATION.value
        return {
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'price': price,
            'timestamp': timestamp,
            'status': status,
            'source': source
        }

    def _fetch_from_api(self) -> Optional[Dict[str, Any]]:
        """Fetch trade from external API (placeholder)"""
        if random.random() < 0.3:  # 30% success rate
            trade_data = self._generate_sample_trade()
            trade_data['source'] = DataSource.API.value
            return trade_data
        return None


# TEST HARNESS (DO NOT MODIFY)
def run_test():
    """Test your implementation"""
    print("Starting TradeIngestionService Test...")

    # Create service with shared queue
    test_queue = queue.Queue()
    service = TradeIngestionService(test_queue)

    # Start service
    service.start()

    # Run for 10 seconds
    time.sleep(10)

    # Stop service
    service.stop()

    # Process remaining trades
    processed = 0
    while not test_queue.empty():
        trade_data = test_queue.get()
        print(f"Trade {trade_data['trade_id']}: {trade_data['symbol']} - {trade_data['quantity']} shares")
        processed += 1

    print(f"\nTest Complete: {processed} trades processed")
    assert processed >= 5, "Not enough trades generated!"


if __name__ == "__main__":
    run_test()