-- Every change event, one row per event (history)
CREATE TABLE IF NOT EXISTS customers_changes (
  op              TEXT,
  id              INT,
  name            TEXT,
  email           TEXT,
  city            TEXT,
  event_ts_ms     BIGINT,
  kafka_partition INT,
  kafka_offset    BIGINT
);

-- Current state of customers, kept in sync with MySQL (inserts, updates, deletes applied)
CREATE TABLE IF NOT EXISTS customers_replica (
  id    INT PRIMARY KEY,
  name  TEXT,
  email TEXT,
  city  TEXT
);
