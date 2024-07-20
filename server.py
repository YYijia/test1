import asyncio
import websockets
import time
import socket
from business import BusinessHandler, ServerInfo
import yaml   

async def handle_client_request(websocket, path):
    global business_handler
    try:
        async for request in websocket:
            address = websocket.remote_address[0]
            if address in business_handler.servers:
                print(f'Receive request from server {address}\nRequest{request}')           
            await business_handler.handle(request, websocket)
            await asyncio.sleep(0.2)

        print(f"{'Server' if address in business_handler.servers else 'Client'} {websocket.remote_address[0]} disconnected") 
        await business_handler.client_left(websocket)   
    except Exception as ex:
        if 'ConnectionClosed' in type(ex).__name__:
            await business_handler.client_left(websocket)
            print(f"{'Server' if address in business_handler.servers else 'Client'} {websocket.remote_address} disconnected")
        else:
            print(f'Error: {str(ex)}')   

async def send_attendance(server_ip_address, websocket):
    global business_handler
    if server_ip_address not in business_handler.servers:
        server = ServerInfo(server_ip_address)
        business_handler.servers[server_ip_address] = server
    server = business_handler.servers[server_ip_address]    
    server.members_info = []
    members = await business_handler.send_attendance(websocket)
    server.members_info.extend(members)
    return server

async def send_check(server, websocket):
    global business_handler
    if server.time_check_alive < time.time():
        response = await business_handler.send_check(websocket)
        if response != 'online':
            print(f"{websocket.remote_address[0]} check result: {response}")
        if response != 'online':
            count_suspend += 1
            return 1
        
        server.status = response
        server.time_check_alive = time.time() + 5

    return 0

async def connect_server(uri):
    successed_connect = False
    server_ip_address = uri[uri.index("//") + 2:uri.rindex(":")]
    while not successed_connect:
        try:
            async with websockets.connect(uri) as websocket:
                print('Connect ' + uri + ' success')
                successed_connect = True
                server = await send_attendance(server_ip_address, websocket)
                count_suspend = 0
                while True:
                    count_suspend += await send_check(server, websocket)
                    if count_suspend > 3:
                        break
                    if server.status == 'online':
                        count_suspend = 0
                        if not server.queue.is_empty():
                            message = server.queue.pop() 
                            await websocket.send(message)
                            print(f'Sent to server {websocket.remote_address[0]}: {message}')
                           
                    await asyncio.sleep(0.5)    
                
        except Exception as ex:
            await asyncio.sleep(1)
            successed_connect = False    
            
async def connect_other_servers():
    global config
    servers_uri = [f"ws://{s['ipAddress']}:{s['port']}" for s in config['groupServers']]
    await asyncio.gather(*[connect_server(uri) for uri in servers_uri]) 
                    

async def start_server():
    global config
    default_address = socket.gethostbyname(socket.gethostname())
    ip_address = config['localServer']['ipAddress'] if config['localServer']['ipAddress'] else default_address
    port = config['localServer']['port']
    async with websockets.serve(handle_client_request, ip_address, port):
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
    
