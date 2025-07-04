# Requirements: pip install websocket-client

import websocket
import threading
import time
import json
import logging
import os
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict, deque
from datetime import datetime, timedelta

# --- Configuration ---
getcontext().prec = 18

SYMBOLS_TO_TRACK = [
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD",
    "DOGE-USD", "ADA-USD", "AVAX-USD", "LINK-USD", "LTC-USD",
    "BCH-USD", "DOT-USD", "EOS-USD", "ETC-USD", "FIL-USD",
    "SUSHI-USD", "AAVE-USD", "XLM-USD", "ATOM-USD", "NEAR-USD",
    "APT-USD", "APE-USD", "ICP-USD", "TRX-USD", "UNI-USD",
    "CRV-USD", "BAT-USD", "COMP-USD", "ZEC-USD", "SNX-USD",
    "YFI-USD", "MANA-USD", "CHZ-USD", "THETA-USD",
    "HBAR-USD", "KNC-USD", "BNB-USD", "NEO-USD",
    "XTZ-USD",
]

COINBASE_WSS_URL       = "wss://ws-feed.exchange.coinbase.com"
BITUNIX_WSS_URL        = "wss://fapi.bitunix.com/public/"
COINBASE_CHANNEL       = "ticker"
BITUNIX_CHANNEL        = "tickers"
DISCREPANCY_THRESHOLD  = Decimal("0.0030")
RECONNECT_DELAY_S      = 5
MAX_RECONNECT_DELAY    = 60
COINBASE_PING_INTERVAL = 20
BITUNIX_PING_INTERVAL  = 20

REPORTING_INTERVAL_MIN = 5
HISTORY_DURATION_MIN = 1
MAX_HISTORY_POINTS   = HISTORY_DURATION_MIN * 60 * 10
MAX_HISTORICAL_INTERVAL_REPORTS = 100

MONITOR_LOOP_INTERVAL_S = 0.1
OP_END_GRACE_PERIOD_CHECKS = 3

# --- Logging Configuration ---
PRE_OPPORTUNITY_BUFFER_SECONDS = 5
POST_OPPORTUNITY_BUFFER_SECONDS = 5
MAX_ORDERBOOK_EVENTS = 1000  # Maximum events to store per symbol

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO, # Ensure INFO level is captured
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logging.getLogger("websocket").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

# --- Shared State ---
latest_market_data = defaultdict(lambda: {
    "coinbase":    {"bid": None, "ask": None, "timestamp": None},
    "bitunix":     {"bid": None, "ask": None, "timestamp": None},
    "discrepancy": None, 
    "discrepancy_cb_bu": None,
    "discrepancy_bu_cb": None,
    "history":   deque(maxlen=MAX_HISTORY_POINTS)
})
data_lock         = threading.Lock()
_opportunity_log  = []
opportunity_lock  = threading.Lock()
interval_report = {"symbol": None, "value": None, "timestamp": None}
interval_report_lock = threading.Lock()
historical_interval_reports = defaultdict(lambda: deque(maxlen=MAX_HISTORICAL_INTERVAL_REPORTS))
historical_interval_reports_lock = threading.Lock()
interval_peaks = defaultdict(lambda: {
    "max_cb_bu": Decimal("-inf"), "min_cb_bu": Decimal("inf"),
    "max_bu_cb": Decimal("-inf"), "min_bu_cb": Decimal("inf")
})
interval_peaks_lock = threading.Lock()
_threads_started  = False
_handlers         = []
_monitor_thread   = None
_reporter_thread  = None

# --- Enhanced Logging Data Structures ---
# Buffer for recent orderbook events (per symbol)
orderbook_event_buffer = defaultdict(lambda: deque(maxlen=MAX_ORDERBOOK_EVENTS))
orderbook_buffer_lock = threading.Lock()

# Active opportunity logs
active_opportunity_logs = {}
active_logs_lock = threading.Lock()

# Completed opportunity logs
completed_opportunity_logs = {}
completed_logs_lock = threading.Lock()

# Post-opportunity logging state
post_opportunity_timers = {}
post_timer_lock = threading.Lock()

# --- Helper ---
def safe_decimal(value, default=Decimal("NaN")):
    if value is None: return default
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else default
    except (ValueError, TypeError):
        return default

# --- Enhanced Logging Functions ---
def log_orderbook_event(symbol, exchange, event_type, data, timestamp=None):
    """
    Log an orderbook event to the appropriate buffer.
    
    Args:
        symbol: Trading pair symbol (e.g., "BTC-USD")
        exchange: Exchange name (e.g., "coinbase", "bitunix")
        event_type: Type of event (e.g., "update", "snapshot")
        data: Orderbook data (dict with bid, ask, etc.)
        timestamp: Event timestamp (defaults to current time if None)
    """
    if timestamp is None:
        timestamp = datetime.now()
        
    event = {
        "timestamp": timestamp,
        "exchange": exchange,
        "event_type": event_type,
        "data": data
    }
    
    # Add to general buffer
    with orderbook_buffer_lock:
        orderbook_event_buffer[symbol].append(event)
    
    # Check if this symbol has an active opportunity being logged
    with active_logs_lock:
        if symbol in active_opportunity_logs:
            active_opportunity_logs[symbol]["events"].append(event)
    
    # Check if this symbol is in post-opportunity logging period
    with post_timer_lock:
        if symbol in post_opportunity_timers:
            opportunity_id, end_time = post_opportunity_timers[symbol]
            if datetime.now() <= end_time:
                # Still in post-opportunity period, add to post events
                with completed_logs_lock:
                    if symbol in completed_opportunity_logs and opportunity_id in completed_opportunity_logs[symbol]:
                        completed_opportunity_logs[symbol][opportunity_id]["post_events"].append(event)
            else:
                # Post-opportunity period has ended, remove timer
                del post_opportunity_timers[symbol]

def start_opportunity_logging(symbol, opportunity_id, opportunity_data):
    """
    Start logging for a detected opportunity, including the pre-opportunity buffer.
    
    Args:
        symbol: Trading pair symbol
        opportunity_id: Unique identifier for the opportunity
        opportunity_data: Additional data about the opportunity
    """
    logging.info(f"Starting opportunity logging for {symbol} (ID: {opportunity_id})")
    
    # Get pre-opportunity events from buffer
    pre_events = []
    with orderbook_buffer_lock:
        buffer = list(orderbook_event_buffer[symbol])
        
    # Filter events within pre-opportunity window
    cutoff_time = datetime.now() - timedelta(seconds=PRE_OPPORTUNITY_BUFFER_SECONDS)
    pre_events = [e for e in buffer if e["timestamp"] >= cutoff_time]
    
    # Create new log entry
    log_entry = {
        "id": opportunity_id,
        "symbol": symbol,
        "start_time": datetime.now(),
        "opportunity_data": opportunity_data,
        "pre_events": pre_events,
        "events": [],
        "end_time": None,
        "end_data": None,
        "post_events": []
    }
    
    # Store in active opportunities
    with active_logs_lock:
        active_opportunity_logs[symbol] = log_entry

def end_opportunity_logging(symbol, opportunity_id, end_data=None):
    """
    End logging for an opportunity but continue for post-opportunity period.
    
    Args:
        symbol: Trading pair symbol
        opportunity_id: Unique identifier for the opportunity
        end_data: Additional data about the opportunity end
    """
    logging.info(f"Ending opportunity logging for {symbol} (ID: {opportunity_id}), starting post-opportunity period")
    
    # Get active log
    with active_logs_lock:
        if symbol not in active_opportunity_logs:
            logging.error(f"No active log found for {symbol} (ID: {opportunity_id})")
            return
            
        log_entry = active_opportunity_logs[symbol]
        log_entry["end_time"] = datetime.now()
        log_entry["end_data"] = end_data
        
        # Move to completed logs
        with completed_logs_lock:
            if symbol not in completed_opportunity_logs:
                completed_opportunity_logs[symbol] = {}
                
            completed_opportunity_logs[symbol][opportunity_id] = log_entry
            
        # Remove from active logs
        del active_opportunity_logs[symbol]
    
    # Start post-opportunity timer
    end_time = datetime.now() + timedelta(seconds=POST_OPPORTUNITY_BUFFER_SECONDS)
    with post_timer_lock:
        post_opportunity_timers[symbol] = (opportunity_id, end_time)

def get_opportunity_logs(symbol=None, opportunity_id=None):
    """
    Get opportunity logs for a specific symbol or opportunity ID.
    
    Args:
        symbol: Trading pair symbol (optional)
        opportunity_id: Unique identifier for the opportunity (optional)
        
    Returns:
        Dictionary of opportunity logs
    """
    result = {}
    
    # Get active logs
    with active_logs_lock:
        if symbol is not None:
            if symbol in active_opportunity_logs:
                active_id = active_opportunity_logs[symbol]["id"]
                result[active_id] = active_opportunity_logs[symbol]
        else:
            for sym, log in active_opportunity_logs.items():
                log_id = log["id"]
                if opportunity_id is None or log_id == opportunity_id:
                    result[log_id] = log
    
    # Get completed logs
    with completed_logs_lock:
        if symbol is not None:
            if symbol in completed_opportunity_logs:
                if opportunity_id is not None:
                    if opportunity_id in completed_opportunity_logs[symbol]:
                        result[opportunity_id] = completed_opportunity_logs[symbol][opportunity_id]
                else:
                    for op_id, log in completed_opportunity_logs[symbol].items():
                        result[op_id] = log
        elif opportunity_id is not None:
            for sym, logs in completed_opportunity_logs.items():
                if opportunity_id in logs:
                    result[opportunity_id] = logs[opportunity_id]
        else:
            for sym, logs in completed_opportunity_logs.items():
                for op_id, log in logs.items():
                    result[op_id] = log
    
    return result

def format_opportunity_log(log_entry):
    """
    Format an opportunity log entry for display.
    
    Args:
        log_entry: Opportunity log entry
        
    Returns:
        Formatted log string
    """
    if not log_entry:
        return "No log data available."
        
    formatted = []
    
    # Header
    formatted.append(f"OPPORTUNITY LOG: {log_entry['symbol']} (ID: {log_entry['id']})")
    formatted.append(f"Detected at: {log_entry['start_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
    
    # Opportunity data
    op_data = log_entry['opportunity_data']
    formatted.append(f"Initial discrepancy: {op_data.get('initial_discrepancy', 'N/A')}%")
    formatted.append(f"Direction: {op_data.get('direction', 'N/A')}")
    formatted.append(f"Price 1: {op_data.get('price1', 'N/A')}")
    formatted.append(f"Price 2: {op_data.get('price2', 'N/A')}")
    formatted.append("")
    
    # Pre-opportunity events
    formatted.append("=== PRE-OPPORTUNITY EVENTS ===")
    for event in log_entry['pre_events']:
        formatted.append(_format_event(event))
    formatted.append("")
    
    # Opportunity events
    formatted.append("=== OPPORTUNITY EVENTS ===")
    for event in log_entry['events']:
        formatted.append(_format_event(event))
    formatted.append("")
    
    # End data if available
    if log_entry['end_time']:
        formatted.append("=== OPPORTUNITY END ===")
        formatted.append(f"End time: {log_entry['end_time'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        
        if log_entry['end_data']:
            end_data = log_entry['end_data']
            formatted.append(f"Duration: {end_data.get('duration_seconds', 'N/A')} seconds")
            formatted.append(f"Peak discrepancy: {end_data.get('peak_pct', 'N/A')}%")
            formatted.append(f"Final discrepancy: {end_data.get('final_discrepancy', 'N/A')}%")
        formatted.append("")
        
        # Post-opportunity events
        formatted.append("=== POST-OPPORTUNITY EVENTS ===")
        for event in log_entry['post_events']:
            formatted.append(_format_event(event))
    
    return "\n".join(formatted)

def _format_event(event):
    """
    Format a single event for display.
    
    Args:
        event: Event data
        
    Returns:
        Formatted event string
    """
    timestamp_str = event["timestamp"].strftime("%H:%M:%S.%f")[:-3]
    exchange = event["exchange"].upper()
    
    data = event["data"]
    if "bid" in data and "ask" in data:
        return f"[{timestamp_str}] {exchange}: Bid={data['bid']}, Ask={data['ask']}"
    else:
        return f"[{timestamp_str}] {exchange}: {event['event_type']}"

# --- WebSocket Handlers ---
class BaseWebSocketHandler(threading.Thread):
    def __init__(self, name, url, symbol, ping_interval):
        super().__init__(name=f"{name}-{symbol.replace('-', '')}", daemon=True)
        self.name            = name
        self.url             = url
        self.original_symbol = symbol
        self.ping_interval   = ping_interval
        self.ws              = None
        self._stop_event     = threading.Event()
        self.reconnect_delay = RECONNECT_DELAY_S

    def connect(self):
        logging.info(f"Attempting to connect to {self.name} for {self.original_symbol}: {self.url}")
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = self._on_error,
            on_close   = self._on_close,
            on_pong    = self._on_pong
        )
        thread = threading.Thread(
            target=lambda: self.ws.run_forever(
                ping_interval=self.ping_interval,
                ping_timeout=10 
            ),
            name=f"{self.name}-{self.original_symbol}-WSLoop",
            daemon=True
        )
        thread.start()

    def _on_open(self, ws):
        logging.info(f"{self.name} WebSocket connected for {self.original_symbol}.")
        self.reconnect_delay = RECONNECT_DELAY_S
        try:
            self.on_open(ws)
        except Exception as e:
            logging.error(f"Error during {self.name}.on_open: {e}", exc_info=True)
            self.stop() 

    def _on_message(self, ws, message):
        # Record application reception time as early as possible
        app_reception_ts = datetime.now()
        try:
            self.on_message(ws, message, app_reception_ts) # Pass app_reception_ts to specific handler
        except Exception as e:
            logging.error(
                f"Unhandled exception in {self.name}.on_message ({self.original_symbol}): {e}\n"
                f"Message snippet: {message[:200]}",
                exc_info=True
            )

    def _on_error(self, ws, error):
        if not self._stop_event.is_set():
            if isinstance(error, (websocket.WebSocketConnectionClosedException, ConnectionResetError, BrokenPipeError)):
                logging.warning(f"{self.name} connection closed/reset/broken pipe ({self.original_symbol}). Will attempt reconnect.")
            elif isinstance(error, websocket.WebSocketTimeoutException):
                 logging.warning(f"{self.name} WebSocket timeout ({self.original_symbol}). Will attempt reconnect.")
            else:
                logging.error(f"{self.name} error for {self.original_symbol}: {error}")

    def _on_close(self, ws, code, msg):
        if self._stop_event.is_set():
            logging.info(f"{self.name} ({self.original_symbol}) stopped gracefully.")
            self.clear_data() 
            return
        logging.warning(f"{self.name} closed ({self.original_symbol}) code={code}, msg='{msg}'. Reconnect in {self.reconnect_delay}s")
        self.clear_data() 
        self.ws = None 
        time.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, MAX_RECONNECT_DELAY)
        if not self._stop_event.is_set(): 
            self.connect()

    def _on_pong(self, ws, message):
        logging.debug(f"{self.name} got pong for {self.original_symbol}")

    def stop(self):
        self._stop_event.set()
        if self.ws:
            threading.Thread(target=self.ws.close, daemon=True).start()

    def clear_data(self):
        with data_lock:
            if self.original_symbol in latest_market_data:
                side = "coinbase" if self.name=="CoinbaseHandler" else "bitunix"
                latest_market_data[self.original_symbol][side] = {"bid":None,"ask":None,"timestamp":None}
                latest_market_data[self.original_symbol]["discrepancy"] = None
                latest_market_data[self.original_symbol]["discrepancy_cb_bu"] = None
                latest_market_data[self.original_symbol]["discrepancy_bu_cb"] = None

    def on_open(self, ws):
        raise NotImplementedError

    def on_message(self, ws, message, app_reception_ts): # Added app_reception_ts
        raise NotImplementedError

    def _update_discrepancy(self, sym, exchange_timestamp): # exchange_timestamp is the ts from the message
        m = latest_market_data[sym]
        cb_a = safe_decimal(m["coinbase"]["ask"], None)
        cb_b = safe_decimal(m["coinbase"]["bid"], None)
        bu_b = safe_decimal(m["bitunix"]["bid"], None)
        bu_a = safe_decimal(m["bitunix"]["ask"], None)

        if not (m["coinbase"]["timestamp"] and m["bitunix"]["timestamp"]):
            m["discrepancy"] = None
            m["discrepancy_cb_bu"] = None
            m["discrepancy_bu_cb"] = None
            return

        pct1 = ((bu_b - cb_a) / cb_a * 100) if cb_a and bu_b and cb_a > 0 else Decimal("-inf")
        pct2 = ((cb_b - bu_a) / bu_a * 100) if bu_a and cb_b and bu_a > 0 else Decimal("-inf")

        m["discrepancy_cb_bu"] = pct1 if pct1 > Decimal("-inf") else None
        m["discrepancy_bu_cb"] = pct2 if pct2 > Decimal("-inf") else None
        
        with interval_peaks_lock:
            if pct1 > Decimal("-inf"):
                interval_peaks[sym]["max_cb_bu"] = max(interval_peaks[sym]["max_cb_bu"], pct1)
                interval_peaks[sym]["min_cb_bu"] = min(interval_peaks[sym]["min_cb_bu"], pct1)
            if pct2 > Decimal("-inf"):
                interval_peaks[sym]["max_bu_cb"] = max(interval_peaks[sym]["max_bu_cb"], pct2)
                interval_peaks[sym]["min_bu_cb"] = min(interval_peaks[sym]["min_bu_cb"], pct2)

        valid_discrepancies = [p for p in (pct1, pct2) if p > Decimal("-inf")]
        disc = max(valid_discrepancies) if valid_discrepancies else Decimal("0") 

        m["discrepancy"] = disc 
        # History uses the timestamp from the exchange message that triggered this update
        m["history"].append((exchange_timestamp, disc))


class CoinbaseHandler(BaseWebSocketHandler):
    def __init__(self, symbol):
        super().__init__("CoinbaseHandler", COINBASE_WSS_URL, symbol, COINBASE_PING_INTERVAL)
        self._ping_thread = None
        self._ping_thread_stop_event = threading.Event()

    def on_open(self, ws):
        # Use nested channel format per Coinbase docs
        sub = {
            "type": "subscribe",
            "channels": [{
                "name": COINBASE_CHANNEL,
                "product_ids": [self.original_symbol]
            }]
        }
        ws.send(json.dumps(sub))
        logging.info(f"Coinbase: Subscribed to ticker for {self.original_symbol}")
        self._start_ping()

    def _start_ping(self):
        self._ping_thread_stop_event.clear()
        def ping_loop():
            time.sleep(self.ping_interval / 2)
            while not self._ping_thread_stop_event.is_set() and self.ws and self.ws.sock and self.ws.sock.connected:
                try:
                    self.ws.send("ping")
                    logging.debug(f"Coinbase ping sent for {self.original_symbol}")
                except Exception as e:
                    if not self._ping_thread_stop_event.is_set():
                        logging.error(f"Error sending Coinbase ping for {self.original_symbol}: {e}")
                    break 
                if self._ping_thread_stop_event.wait(self.ping_interval):
                    break
            logging.info(f"Coinbase ping loop stopped for {self.original_symbol}")
        self._ping_thread = threading.Thread(target=ping_loop, name=f"{self.name}-{self.original_symbol}-PingLoop", daemon=True)
        self._ping_thread.start()

    def stop(self):
        self._ping_thread_stop_event.set() 
        if self._ping_thread and self._ping_thread.is_alive():
            self._ping_thread.join(timeout=5) 
        super().stop() 

    def on_message(self, ws, message, app_reception_ts):
        if message == "pong":
            logging.debug(f"Coinbase pong received for {self.original_symbol} at {app_reception_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}")
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logging.error(f"Coinbase JSON decode error for {self.original_symbol} (AppTS: {app_reception_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}): {message[:100]}")
            return

        if data.get("type") != "ticker":
            return

        bid = data.get("best_bid")
        ask = data.get("best_ask")
        ts  = data.get("time")
        if not (bid and ask and ts):
            return

        try:
            # Convert ISO timestamp to datetime
            exchange_ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            best_bid_price = safe_decimal(bid)
            best_ask_price = safe_decimal(ask)
        except Exception as e:
            logging.error(f"Error parsing Coinbase payload for {self.original_symbol} (AppTS: {app_reception_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}): {e} - Data: {data}")
            return

        # Log orderbook event for verification
        orderbook_data = {
            "bid": best_bid_price,
            "ask": best_ask_price,
            "app_ts": app_reception_ts,
            "exchange_ts": exchange_ts_dt
        }
        log_orderbook_event(
            symbol=self.original_symbol,
            exchange="coinbase",
            event_type="update",
            data=orderbook_data,
            timestamp=app_reception_ts
        )

        logging.info(f"COINBASE_RECV_DATA: Sym={self.original_symbol}, AppTS={app_reception_ts.strftime('%H:%M:%S.%f')}, ExchTS={exchange_ts_dt.strftime('%H:%M:%S.%f')}, Bid={best_bid_price}, Ask={best_ask_price}")

        with data_lock:
            latest_market_data[self.original_symbol]["coinbase"] = {"bid": best_bid_price, "ask": best_ask_price, "timestamp": exchange_ts_dt}
            self._update_discrepancy(self.original_symbol, exchange_ts_dt)


class BitunixHandler(BaseWebSocketHandler):
    def __init__(self, symbol):
        super().__init__(
            "BitunixHandler",
            BITUNIX_WSS_URL,
            symbol,
            BITUNIX_PING_INTERVAL
        )
        self.bitunix_symbol = symbol.replace("-", "") 

    def on_open(self, ws):
        ws.send(json.dumps({
            "op": "subscribe",
            "args": [
                {"symbol": self.bitunix_symbol, "ch": BITUNIX_CHANNEL}
            ]
        }))
        logging.info(f"Bitunix: Subscribed to tickers for {self.bitunix_symbol}")

    def on_message(self, ws, message, app_reception_ts):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logging.error(f"Bitunix JSON decode error for {self.original_symbol} (AppTS: {app_reception_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}): {message[:100]}")
            return

        if data.get("op") == "ping":
            ws.send(json.dumps({"op": "pong"}))
            return
            
        if data.get("ch") == BITUNIX_CHANNEL and isinstance(data.get("data"), list):
            for entry in data["data"]:
                if entry.get("s") == self.bitunix_symbol:
                    bid = entry.get("bd")
                    ask = entry.get("ak")
                    ts  = data.get("ts")
                    if not (bid and ask and ts):
                        continue
                        
                    try:
                        exchange_ts_dt = datetime.fromtimestamp(int(ts)/1000.0, tz=None)
                        best_bid_price = safe_decimal(bid)
                        best_ask_price = safe_decimal(ask)
                    except Exception as e:
                        logging.error(f"Error parsing Bitunix payload for {self.original_symbol} (AppTS: {app_reception_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}): {e} - Data: {entry}")
                        continue

                    # Log orderbook event for verification
                    orderbook_data = {
                        "bid": best_bid_price,
                        "ask": best_ask_price,
                        "app_ts": app_reception_ts,
                        "exchange_ts": exchange_ts_dt
                    }
                    log_orderbook_event(
                        symbol=self.original_symbol,
                        exchange="bitunix",
                        event_type="update",
                        data=orderbook_data,
                        timestamp=app_reception_ts
                    )

                    logging.info(f"BITUNIX_RECV_DATA: Sym={self.original_symbol}, AppTS={app_reception_ts.strftime('%H:%M:%S.%f')}, ExchTS={exchange_ts_dt.strftime('%H:%M:%S.%f')}, Bid={best_bid_price}, Ask={best_ask_price}")

                    with data_lock:
                        latest_market_data[self.original_symbol]["bitunix"] = {"bid": best_bid_price, "ask": best_ask_price, "timestamp": exchange_ts_dt}
                        self._update_discrepancy(self.original_symbol, exchange_ts_dt)
                    break


class Monitor(threading.Thread):
    def __init__(self, symbols):
        super().__init__(name="Monitor", daemon=True)
        self.symbols = symbols
        self._stop_event = threading.Event()
        self.active_opportunities = {}
        self.grace_period_counters = {}
        self.opportunity_counter = 0

    def run(self):
        logging.info("Monitor thread started.")
        while not self._stop_event.wait(MONITOR_LOOP_INTERVAL_S):
            try:
                self._check_opportunities()
            except Exception as e:
                logging.error(f"Error in Monitor._check_opportunities: {e}", exc_info=True)
        logging.info("Monitor thread stopped.")

    def _check_opportunities(self):
        with data_lock:
            for symbol in self.symbols:
                data = latest_market_data.get(symbol, {})
                disc = safe_decimal(data.get("discrepancy"))
                disc_cb_bu = safe_decimal(data.get("discrepancy_cb_bu"))
                disc_bu_cb = safe_decimal(data.get("discrepancy_bu_cb"))
                
                # Skip if no valid discrepancy data
                if disc.is_nan():
                    continue
                
                # Check if symbol is in active opportunity
                if symbol in self.active_opportunities:
                    op_data = self.active_opportunities[symbol]
                    peak_pct = max(op_data["peak_pct"], disc)
                    
                    # Update peak if current discrepancy is higher
                    if disc > op_data["peak_pct"]:
                        op_data["peak_pct"] = disc
                        op_data["peak_time"] = datetime.now()
                        
                        # Update prices at peak
                        op_data["price1_at_peak"] = data.get("coinbase", {}).get("ask") if op_data["direction"] == "cb_to_bu" else data.get("bitunix", {}).get("ask")
                        op_data["price2_at_peak"] = data.get("bitunix", {}).get("bid") if op_data["direction"] == "cb_to_bu" else data.get("coinbase", {}).get("bid")
                    
                    # Check if opportunity has ended (discrepancy below threshold)
                    if disc < DISCREPANCY_THRESHOLD:
                        if symbol not in self.grace_period_counters:
                            self.grace_period_counters[symbol] = 1
                        else:
                            self.grace_period_counters[symbol] += 1
                            
                        # End opportunity after grace period
                        if self.grace_period_counters[symbol] >= OP_END_GRACE_PERIOD_CHECKS:
                            end_time = datetime.now()
                            duration = end_time - op_data["start"]
                            
                            # Log opportunity end
                            logging.info(f"OPPORTUNITY ENDED: {symbol} - Peak: {op_data['peak_pct']:.4f}% - Duration: {duration.total_seconds():.2f}s")
                            
                            # Record in opportunity log
                            with opportunity_lock:
                                _opportunity_log.append({
                                    "symbol": symbol,
                                    "start": op_data["start"],
                                    "end": end_time,
                                    "duration": duration,
                                    "peak_pct": op_data["peak_pct"],
                                    "peak_time": op_data["peak_time"],
                                    "direction": op_data["direction"],
                                    "price1": op_data["price1"],
                                    "price2": op_data["price2"],
                                    "price1_at_peak": op_data["price1_at_peak"],
                                    "price2_at_peak": op_data["price2_at_peak"],
                                    "id": op_data["id"]
                                })
                            
                            # End opportunity logging
                            end_data = {
                                "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                                "duration_seconds": duration.total_seconds(),
                                "peak_pct": float(op_data["peak_pct"]),
                                "final_discrepancy": float(disc)
                            }
                            end_opportunity_logging(symbol, op_data["id"], end_data)
                            
                            # Clean up
                            del self.active_opportunities[symbol]
                            if symbol in self.grace_period_counters:
                                del self.grace_period_counters[symbol]
                    else:
                        # Reset grace period counter if discrepancy is above threshold
                        if symbol in self.grace_period_counters:
                            del self.grace_period_counters[symbol]
                
                # Check for new opportunity
                elif disc >= DISCREPANCY_THRESHOLD:
                    start_time = datetime.now()
                    self.opportunity_counter += 1
                    opportunity_id = f"op_{self.opportunity_counter}_{int(time.time())}"
                    
                    # Determine direction of opportunity
                    direction = "cb_to_bu" if disc_cb_bu and disc_cb_bu > 0 and (not disc_bu_cb or disc_cb_bu >= disc_bu_cb) else "bu_to_cb"
                    
                    # Get prices
                    price1 = data.get("coinbase", {}).get("ask") if direction == "cb_to_bu" else data.get("bitunix", {}).get("ask")
                    price2 = data.get("bitunix", {}).get("bid") if direction == "cb_to_bu" else data.get("coinbase", {}).get("bid")
                    
                    # Log new opportunity
                    logging.info(f"NEW OPPORTUNITY: {symbol} - Initial: {disc:.4f}% - Direction: {direction}")
                    
                    # Record active opportunity
                    self.active_opportunities[symbol] = {
                        "start": start_time,
                        "peak_pct": disc,
                        "peak_time": start_time,
                        "direction": direction,
                        "price1": price1,
                        "price2": price2,
                        "price1_at_peak": price1,
                        "price2_at_peak": price2,
                        "id": opportunity_id
                    }
                    
                    # Start opportunity logging
                    opportunity_data = {
                        "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                        "initial_discrepancy": float(disc),
                        "direction": direction,
                        "price1": str(price1),
                        "price2": str(price2),
                        "threshold": float(DISCREPANCY_THRESHOLD)
                    }
                    start_opportunity_logging(symbol, opportunity_id, opportunity_data)

    def stop(self):
        self._stop_event.set()


class IntervalReporter(threading.Thread):
    def __init__(self, symbols, monitor_thread, interval_minutes=REPORTING_INTERVAL_MIN):
        super().__init__(name="IntervalReporter", daemon=True)
        self.symbols = symbols
        self.monitor_thread = monitor_thread
        self.interval_seconds = interval_minutes * 60
        self._stop_event = threading.Event()

    def run(self):
        logging.info(f"IntervalReporter thread started with {self.interval_seconds}s interval.")
        while not self._stop_event.wait(self.interval_seconds):
            try:
                self._generate_report()
            except Exception as e:
                logging.error(f"Error in IntervalReporter._generate_report: {e}", exc_info=True)
        logging.info("IntervalReporter stopped.")

    def _generate_report(self):
        report_time = datetime.now()
        logging.info(f"Generating interval report at {report_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        highest_overall_positive_disc = Decimal("-inf")
        highest_overall_positive_symbol = None
        least_overall_negative_disc = Decimal("-inf")
        least_overall_negative_symbol = None
        found_any_overall_valid_disc = False
        
        with interval_peaks_lock:
            for s in self.symbols:
                symbol_peaks = interval_peaks[s]
                is_in_op = s in self.monitor_thread.active_opportunities
                
                peak_cb_bu = symbol_peaks["max_cb_bu"] if symbol_peaks["max_cb_bu"] > Decimal("-inf") else None
                peak_bu_cb = symbol_peaks["max_bu_cb"] if symbol_peaks["max_bu_cb"] > Decimal("-inf") else None
                
                if not is_in_op:
                    if peak_cb_bu is not None or peak_bu_cb is not None: 
                        with historical_interval_reports_lock:
                            historical_interval_reports[s].append({"timestamp": report_time, "cb_to_bu": peak_cb_bu, "bu_to_cb": peak_bu_cb})
                        logging.debug(f"Stored historical interval PEAK for {s}: CB->BU={peak_cb_bu}, BU->CB={peak_bu_cb} at {report_time}")
                    
                    valid_peaks_for_symbol = [p for p in (peak_cb_bu, peak_bu_cb) if p is not None and not p.is_nan()]
                    if valid_peaks_for_symbol:
                        max_peak_for_symbol = max(valid_peaks_for_symbol)
                        found_any_overall_valid_disc = True
                        
                        if max_peak_for_symbol > 0: 
                            if max_peak_for_symbol > highest_overall_positive_disc:
                                highest_overall_positive_disc = max_peak_for_symbol
                                highest_overall_positive_symbol = s
                        else: 
                            if highest_overall_positive_symbol is None: 
                                if least_overall_negative_disc == Decimal("-inf") or max_peak_for_symbol > least_overall_negative_disc:
                                    least_overall_negative_disc = max_peak_for_symbol
                                    least_overall_negative_symbol = s
                elif is_in_op:
                     logging.debug(f"Symbol {s} is in active opportunity, EXCLUDING from interval peak report summary & history.")
            
            # Reset peaks for next interval
            interval_peaks.clear()
            
        final_overall_symbol = None
        final_overall_value = None
        
        if highest_overall_positive_symbol is not None: 
            final_overall_symbol = highest_overall_positive_symbol
            final_overall_value = highest_overall_positive_disc
            logging.info(f"Overall Interval Report: Highest positive peak {final_overall_value:.4f}% for {final_overall_symbol} (excluding active ops).")
        elif least_overall_negative_symbol is not None and least_overall_negative_disc > Decimal("-inf"): 
            final_overall_symbol = least_overall_negative_symbol
            final_overall_value = least_overall_negative_disc
            logging.info(f"Overall Interval Report: No positive peaks. Least negative peak is {final_overall_value:.4f}% for {final_overall_symbol} (excluding active ops).")
        elif found_any_overall_valid_disc: 
             logging.info("Overall Interval Report: No relevant positive or negative peaks found (excluding active ops).")
        else: 
            logging.info("Overall Interval Report: No discrepancy peak data available for any symbol (excluding active ops).")
            
        if final_overall_symbol is not None and final_overall_value is not None:
            with interval_report_lock:
                interval_report["symbol"] = final_overall_symbol
                interval_report["value"] = final_overall_value
                interval_report["timestamp"] = report_time

    def stop(self):
        self._stop_event.set()

# --- Data Access Functions ---
def get_interval_report():
    with interval_report_lock: return dict(interval_report)

def get_opportunity_log():
    with opportunity_lock: return list(_opportunity_log)

def get_historical_interval_reports(symbol=None):
    with historical_interval_reports_lock:
        if symbol: return list(historical_interval_reports.get(symbol, deque()))
        else: return {s: list(hist_deque) for s, hist_deque in historical_interval_reports.items()}

# --- Thread Management ---
def start_threads():
    global _threads_started, _handlers, _monitor_thread, _reporter_thread
    if _threads_started:
        logging.warning("Threads already started.")
        return
    
    logging.info("Initializing threads...")
    _handlers.clear()
    
    with interval_report_lock: 
        interval_report["symbol"] = None
        interval_report["value"] = None
        interval_report["timestamp"] = None
    
    with historical_interval_reports_lock: 
        historical_interval_reports.clear()
    
    with interval_peaks_lock: 
        interval_peaks.clear()
    
    _monitor_thread = Monitor(SYMBOLS_TO_TRACK)
    _monitor_thread.start()
    logging.info("Monitor thread started.")
    
    _reporter_thread = IntervalReporter(SYMBOLS_TO_TRACK, _monitor_thread) 
    _reporter_thread.start()
    logging.info("Reporter thread started.")
    
    logging.info("Starting WebSocket handlers...")
    for sym in SYMBOLS_TO_TRACK:
        _handlers.append(CoinbaseHandler(sym))
        _handlers.append(BitunixHandler(sym))
    
    for h in _handlers:
        h.connect() 
        time.sleep(0.2) 
    
    _threads_started = True
    logging.info("All threads initialized and handlers connecting/connected.")

def stop_threads():
    global _threads_started, _handlers, _monitor_thread, _reporter_thread
    if not _threads_started:
        logging.warning("Threads are not running.")
        return
    
    logging.info("Stopping threads...")
    if _reporter_thread: _reporter_thread.stop()
    if _monitor_thread: _monitor_thread.stop()
    
    logging.info("Stopping WebSocket handlers...")
    for h in _handlers: 
        h.stop()
    
    if _reporter_thread and _reporter_thread.is_alive():
        logging.info("Waiting for Reporter thread to join...")
        _reporter_thread.join(timeout=10) 
        if _reporter_thread.is_alive(): logging.warning("Reporter thread did not join cleanly.")
    
    if _monitor_thread and _monitor_thread.is_alive():
        logging.info("Waiting for Monitor thread to join...")
        _monitor_thread.join(timeout=10) 
        if _monitor_thread.is_alive(): logging.warning("Monitor thread did not join cleanly.")
    
    logging.info("Waiting for WebSocket connections to close...")
    time.sleep(3) 
    
    _threads_started = False
    _handlers.clear() 
    _monitor_thread = None 
    _reporter_thread = None
    
    logging.info("All threads stopped and cleaned up.")

# --- Main Execution ---
if __name__ == "__main__":
    start_threads()
    try:
        while True:
            if _monitor_thread and not _monitor_thread.is_alive():
                logging.error("Monitor thread died unexpectedly!")
                break 
            if _reporter_thread and not _reporter_thread.is_alive():
                logging.error("Reporter thread died unexpectedly!")
                break
            time.sleep(60) 
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received.")
    finally:
        stop_threads()
        logging.info("Detector script finished.")