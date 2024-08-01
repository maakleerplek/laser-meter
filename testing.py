import minimalmodbus
import serial
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import os

# InfluxDB configurations
influxdb_url = "http://localhost:8086"  # Replace with your InfluxDB URL
influxdb_token = os.getenv("INFLUXDB_TOKEN")  # Fetch the token from the environment
influxdb_org = "maakleerplek vzw"  # Replace with your InfluxDB organization
influxdb_bucket = os.getenv("INFLUXDB_BUCKET")  # Replace with your InfluxDB bucket

# SDM120 Modbus configuration
sdm120_address = 1  # Modbus address of your SDM120
sdm120_port = "/dev/ttyUSB0"  # Replace with your serial port
sdm120_baudrate = 2400  # Default baud rate for SDM120

# Create an instance of the InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Create a Modbus instrument object
instrument = minimalmodbus.Instrument(sdm120_port, sdm120_address)
instrument.serial.baudrate = sdm120_baudrate
instrument.serial.bytesize = 8
instrument.serial.parity = serial.PARITY_NONE
instrument.serial.stopbits = 1
instrument.serial.timeout = 1


# Function to read data from SDM120
def read_sdm120_data():
    try:
        voltage = instrument.read_float(0, functioncode=4)  # Voltage register
        current = instrument.read_float(6, functioncode=4)  # Current register
        active_power = instrument.read_float(
            12, functioncode=4
        )  # Active Power register
        return voltage, current, active_power
    except Exception as e:
        print(f"Error reading from SDM120: {e}")
        return None, None, None


# Function to write data to InfluxDB
def write_to_influxdb(voltage, current, power):
    if None in (voltage, current, power):
        print("Invalid data, not writing to InfluxDB.")
        return

    # Get the current time in UTC
    current_time = time.time_ns()  # Time in nanoseconds

    # Create a Point with data
    point = (
        Point("energy_data")
        .tag("location", "your_location")
        .field("voltage", voltage)
        .field("current", current)
        .field("power", power)
        .time(current_time, WritePrecision.NS)
    )

    # Write the Point to InfluxDB
    try:
        write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)
        print(
            f"Data written to InfluxDB: Voltage={voltage}V, Current={current}A, Power={power}W"
        )
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")


# Main loop to read and write data
while True:
    voltage, current, active_power = read_sdm120_data()
    print(f"Voltage: {voltage} V, Current: {current} A, Power: {active_power} W")
    write_to_influxdb(voltage, current, active_power)
    time.sleep(60)  # Wait for a minute before polling again
