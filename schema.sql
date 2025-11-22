-- Database schema for Public Transportation Analysis
-- Run this script to set up the required tables

CREATE TABLE IF NOT EXISTS vehicle (
  id VARCHAR PRIMARY KEY,
  label VARCHAR,
  vehicle_type VARCHAR
);

CREATE TABLE IF NOT EXISTS vehicle_state (
  id SERIAL PRIMARY KEY,
  vehicle_id VARCHAR REFERENCES vehicle(id),
  latitude DECIMAL,
  longitude DECIMAL,
  timestamp TIMESTAMP,
  speed DECIMAL,
  bike_accessible BOOLEAN,
  wheelchair_accessible BOOLEAN
);

-- Create index on vehicle_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_vehicle_state_vehicle_id ON vehicle_state(vehicle_id);

-- Create index on timestamp for time-based queries
CREATE INDEX IF NOT EXISTS idx_vehicle_state_timestamp ON vehicle_state(timestamp);

