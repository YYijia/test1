import asyncio
import websockets
import time
import threading
import json
import sys
import socket
import traceback
from business import BusinessHandler, ServerInfo
import yaml   

# This function handles incoming messages from clients
async def handle_client(websocket, path):
    global business_handler
    try:
        while True:
            request = await websocket.recv()
            await business_handler.handle(request, websocket)
            await asyncio.sleep(0.2)
    except Exception as ex:
        if 'ConnectionClosed' in type(ex).__name__:
            await business_handler.client_left(websocket)
            print(f"Client disconnected: {websocket.remote_address}")
        else:
            print(f'Error: {str(ex)}')    

async def connect_server(uri):
    successed_connect = False
    address = uri[uri.index("//") + 2:uri.rindex(":")]
    port = int(uri[uri.rindex(":") + 1:])
    while not successed_connect:
        try:
            async with websockets.connect(uri, ping_interval=60, ping_timeout=180) as websocket:
                print('Connect ' + uri + ' success')
                global business_handler
                successed_connect = True
                server = ServerInfo(address, port)
                business_handler.servers[address] = server
                members = await business_handler.send_attendance(websocket)
                server.members_info.extend(members)

                while True:
                    if server.time_check_alive < time.time():
                        response = await business_handler.send_check(websocket)
                        server.status = 'online' if response else 'offline'
                        server.time_check_alive += 5
                    if server.status == 'online' and not server.queue.is_empty():
                        message = server.queue.pop() 
                        await websocket.send(message)   
                    await asyncio.sleep(0.5)    
                
        except Exception as ex:
            if not successed_connect:
                await asyncio.sleep(2)
            
async def connect_other_servers():
    global config
    servers_uri = [f"ws://{s['ipAddress']}:{s['port']}" for s in config['groupServers']]
    await asyncio.gather(*[connect_server(uri) for uri in servers_uri]) 
                    

async def start_server():
    global config
    ip_address = config['localServer']['ipAddress'] if config['localServer']['ipAddress'] else socket.gethostbyname(socket.gethostname())
    port = config['localServer']['port']
    async with websockets.serve(handle_client, ip_address, port):
        print(f"WebSocket server started on ws://{ip_address}:" + str(port))
        # Keep the server running forever
        await asyncio.Future()  

business_handler = None 
config = None

async def main():
    global config, business_handler
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    business_handler = BusinessHandler(config)     
    await asyncio.gather(start_server(), connect_other_servers())        


if __name__ == "__main__":
    asyncio.run(main())
    
