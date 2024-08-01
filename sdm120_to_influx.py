import minimalmodbus
import serial
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time
import os

# InfluxDB configurations
influxdb_url = "http://localhost:8086"  # Replace with your InfluxDB URL
influxdb_token = os.getenv("INFLUXDB_TOKEN")  # Fetch the token from the environment
influxdb_org = os.getenv("INFLUXDB_ORG")  # Replace with your InfluxDB organization
influxdb_bucket = os.getenv("INFLUXDB_BUCKET")  # Replace with your InfluxDB bucket

# SDM120 Modbus configuration
sdm120_address = 1  # Modbus address of your SDM120
sdm120_port = "/dev/ttyUSB0"  # Replace with your serial port
sdm120_baudrate = 2400  # Default baud rate for SDM120

# Create an instance of the InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Create a Modbus instrument object
e_meter = minimalmodbus.Instrument(sdm120_port, sdm120_address)
e_meter.serial.baudrate = sdm120_baudrate
e_meter.serial.bytesize = 8
e_meter.serial.parity = serial.PARITY_NONE
e_meter.serial.stopbits = 1
e_meter.serial.timeout = 1


# Function to read data from SDM120
def read_sdm120_data():
    try:
        # Read values from all registers
        voltage = e_meter.read_float(
            30001 - 1, functioncode=4
        )  # 30001 in 0-based index
        current = e_meter.read_float(
            30007 - 1, functioncode=4
        )  # 30007 in 0-based index
        active_power = e_meter.read_float(
            30013 - 1, functioncode=4
        )  # 30013 in 0-based index
        apparent_power = e_meter.read_float(
            30019 - 1, functioncode=4
        )  # 30019 in 0-based index
        reactive_power = e_meter.read_float(
            30025 - 1, functioncode=4
        )  # 30025 in 0-based index
        power_factor = e_meter.read_float(
            30031 - 1, functioncode=4
        )  # 30031 in 0-based index
        frequency = e_meter.read_float(
            30071 - 1, functioncode=4
        )  # 30071 in 0-based index
        import_active_energy = e_meter.read_float(
            30073 - 1, functioncode=4
        )  # 30073 in 0-based index
        export_active_energy = e_meter.read_float(
            30075 - 1, functioncode=4
        )  # 30075 in 0-based index
        import_reactive_energy = e_meter.read_float(
            30077 - 1, functioncode=4
        )  # 30077 in 0-based index
        export_reactive_energy = e_meter.read_float(
            30079 - 1, functioncode=4
        )  # 30079 in 0-based index

        return {
            "voltage": voltage,
            "current": current,
            "active_power": active_power,
            "apparent_power": apparent_power,
            "reactive_power": reactive_power,
            "power_factor": power_factor,
            "frequency": frequency,
            "import_active_energy": import_active_energy,
            "export_active_energy": export_active_energy,
            "import_reactive_energy": import_reactive_energy,
            "export_reactive_energy": export_reactive_energy,
        }
    except Exception as e:
        print(f"Error reading from SDM120: {e}")
        return None


# Function to write data to InfluxDB
def write_to_influxdb(data):
    if data is None:
        print("No data to write to InfluxDB.")
        return

    # Get the current time in UTC
    current_time = time.time_ns()  # Time in nanoseconds

    # Create and write a Point for each field
    for key, value in data.items():
        point = (
            Point("energy_data")
            .tag("location", "your_location")
            .field(key, value)
            .time(current_time, WritePrecision.NS)
        )

        try:
            write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)
            print(f"Data written to InfluxDB: {key}={value}")
        except Exception as e:
            print(f"Error writing {key} to InfluxDB: {e}")


# Main loop to read and write data
while True:
    data = read_sdm120_data()
    write_to_influxdb(data)
    time.sleep(0.5)
