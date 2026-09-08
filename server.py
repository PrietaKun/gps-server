import asyncio
import psycopg2
from datetime import datetime

HOST = "0.0.0.0"
PORT = 5000


def crc_itu(data: bytes) -> int:
    crc = 0xFFFF

    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0x8408
            else:
                crc >>= 1
            crc &= 0xFFFF
    return crc ^ 0xFFFF

def save_position(device_id, latitude, longitude, speed):
    conn = psycopg2.connect(
        dbname="gpsdb",
        user="postgres",
        host="localhost"
    ) 
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("""INSERT INTO positions
                                  (device_id, latitude, longitude, speed)
                                  VALUES (%s, %s, %s, %s)""",
                               (device_id, latitude, longitude, speed)
                )
    finally:
        conn.close()


async def handle_device(reader, writer):
    address = writer.get_extra_info("peername")
    print (f"[+] GPS conectado: {address}")
    device_id = None
    buffer = b''
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            buffer += data
            while len(buffer) >= 3:
                if buffer[0:2] != b'\x78\x78':
                    buffer = buffer[1:]
                    continue
                length = buffer[2] + 5
                if length < 10:
                    buffer = buffer[2:]
                    continue
                if len(buffer) < length:
                    break
                data = buffer[:length]
                if data[-2:] != b'\x0D\x0A' or crc_itu(data[2:-4]) != int.from_bytes(data[-4:-2], 'big'):
                    buffer = buffer[1:]
                    continue
                buffer = buffer[length:]
                trama_hex = " ".join(f"{byte:02X}" for byte in data)
                print (f"[{datetime.now().isoformat()}] {address} -> {trama_hex}")
                if len(data) >= 18 and data[0:2] == b'\x78\x78' and data[3] == 0x01:
                    device_id = data[4:12].hex().lstrip('0') or '0'
                    serial = data[-6:-4]
                    body = b'\x05\x01' + serial
                    crc = crc_itu(body)

                    response = (b'\x78\x78' + body + crc.to_bytes(2, 'big') + b'\x0D\x0A')
                    writer.write(response)
                    await writer.drain()

                    print (f"[+] LOGIN RESPONDIDO -> {response.hex(' ').upper()}")

                    v3_command = bytes.fromhex("78 78 18 80 12 00 00 00 01"
                                              "53 5A 43 53 23 47 54 30 36 53 45 4C 3D 31"
                                              "00 1F 0D E6 0D 0A")
                    body = bytes([len(v3_command) - 5]) + v3_command[3:-4]
                    crc = crc_itu(body)
                    v3_command = b'\x78\x78' + body + crc.to_bytes(2, 'big') + b'\x0D\x0A'
                    writer.write(v3_command)
                    await writer.drain()

                    print (f"[+] COMANDO V3 ENVIADO -> {v3_command.hex(' ').upper()}")

                if len(data) >= 14 and data[0:2] == b'\x78\x78' and data [3]  == 0x13:
                    serial = data[-6:-4]
                    body = b'\x05\x13' + serial
                    crc = crc_itu(body)

                    response = (b'\x78\x78' + body + crc.to_bytes(2, 'big') + b'\x0D\x0A')
                    writer.write(response)
                    await writer.drain()
                    print (f"[+] HEARTBEAT RESPONDIDO -> {response.hex(' ').upper()}")

                if len(data) >= 36 and data[0:2] == b'\x78\x78' and data[3] == 0x12:
                    lat_raw = int.from_bytes(data[11:15], 'big')
                    lon_raw = int.from_bytes(data[15:19], 'big')

                    latitude = lat_raw / 1800000
                    longitude = lon_raw / 1800000

                    course_status = int.from_bytes(data[20:22], 'big')

                    west = bool(course_status & 0x0800)
                    north = bool(course_status & 0x0400)

                    if west:
                        longitude = -longitude
                    else:
                        longitude = longitude

                    if north:
                        latitude =  latitude
                    else:
                        latitude = - latitude

                    speed = data[19]
                    print(f"[GPS] Latitud: {latitude:.6f}, Longitud: {longitude:.6f}, Velocidad: {speed} km/h")
                    if device_id is not None:
                        try:
                            await asyncio.to_thread(save_position, device_id, latitude, longitude, speed)
                        except Exception as error:
                            print (f"[!] Error al guardar: {error}")



    except Exception as error:
        print (f"[!] Error: {error}")

    finally:
        print (f"[-] GPS desconectado: {address}")
        writer.close()
        await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_device, HOST, PORT)

    print(f"Servidor GPS en {HOST}:{PORT}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
