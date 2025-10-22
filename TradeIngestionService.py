"""
Implement the TradeIngestionService class below using producer-consumer pattern.

Requirements:
1. Thread-safe operations
2. Validate trades before enqueuing
3. Support start/stop lifecycle
4. Graceful shutdown
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
            'symbol': self.symbol.value,
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
    TODO: Implement the following EMPTY methods:
    1. __init__() - Initialize queue, threads, counters
    2. start() - Start producer thread
    3. stop() - Gracefully stop service
    4. _producer_loop() - Generate/validate/enqueue trades
    5. _validate_trade() - Validate trade data
    6. _fetch_from_api() - Simulate API fetch (placeholder)
    """

    def __init__(self, queue: Optional[queue.Queue] = None):
        # TODO: Initialize service components




        pass

    def start(self):
        """Start the ingestion service"""
        # TODO: Start producer thread







        pass

    def stop(self):
        """Gracefully stop the service"""
        # TODO: Stop threads and cleanup






        pass


    def _producer_loop(self):
        """Main producer loop - fetch, validate, enqueue trades"""
        # TODO: Implement producer-consumer logic

























        pass

    def _validate_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Validate trade data before enqueuing"""
        # TODO: Implement validation rules
        # Rules:
        # - quantity > 0
        # - price > 0
        # - symbol in VALID_SYMBOLS
        # - side in [TradeSide.BUY, TradeSide.SELL]
        # - status in [TradeStatus.FILLED, TradeStatus.PARTIAL, TradeStatus.CANCELED]







        pass

    def _fetch_from_api(self) -> Optional[Dict[str, Any]]:
        """Fetch trade from external API (placeholder)"""
        # TODO: Simulate API call, process the result
        # Return None if no trade available











        pass


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