import asyncio
import websockets
import time
import json
import threading
from cryptography.hazmat.primitives import hashes   
from cryptography.hazmat.primitives.asymmetric import padding, rsa  
from cryptography.hazmat.primitives import serialization 

class Queue:
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()

    def push(self, message):
        with self.lock:
            self.queue.append(message)    

    def pop(self):
        with self.lock:
            return self.queue.pop()
        
    def is_empty(self):
        with self.lock:
            return len(self.queue) == 0 

class Member:
    def __init__(self, jid, nickname, private_key, public_key):
        self.jid = jid
        self.nickname = nickname if nickname else ''
        self.private_key = private_key
        self.public_key = public_key

class Configuration:
    def __init__(self, domain, port, server_addresses):
        self.domain = domain
        self.port = port
        self.server_addresses = server_addresses        
    
class ServerInfo:
    def __init__(self, ip_address):    
        self.ip_address = ip_address
        self.queue = Queue()
        self.members_info = []
        self.time_check_alive = time.time() + 5
        self.status = 'online'

class BusinessHandler:
    def __init__(self, config):
        self.config = config
        self.lock = asyncio.Lock()
        self.clients = {}      
        self.servers = {}    
        self.processors = {}
        self.replies = {}
        self.processors['join'] = self._client_join
        self.processors['attendance'] = self._server_join
        self.processors['members'] = self._return_members
        self.processors['check'] = self._check_alive
        self.processors['get_replies'] = self._get_replies
        self.processors['send_message'] = self._send_message
        self.processors['send_file'] = self._send_file
        self.processors['presence'] = self._members_changed
        self.processors['message'] = self._receive_message
        self.processors['file'] = self._receive_message

    async def send_check(self, websocket):    
        try: 
            await websocket.send('{"tag": "check"}')
            response = json.loads(await websocket.recv())
            if 'tag' in response and response['tag'] == 'checked':
                return 'online'
            return 'suspend'
        except Exception as ex:
            return 'suspend'
        
    async def send_attendance(self, websocket):  
        await websocket.send('{"tag": "attendance"}')
        response = json.loads(await websocket.recv())       
        return response['presence']

    async def _members_changed(self, request, websocket):
        self.servers[websocket.remote_address[0]].members_info = request['presence']
    
    async def _server_join(self, request, websocket):
        message = await self._create_precense_message()
        await websocket.send(message)

    async def _send_message(self, request, websocket):
        request['tag'] = 'message'
        await self._send_content(request, websocket)

    async def _send_file(self, request, websocket):
        request['tag'] = 'file'
        await self._send_content(request, websocket)    

    async def _send_content(self, request, websocket):
        to = request['to']
        for server in self.servers.values():
            for m in server.members_info:
                if m['jid'] == to or to == 'public':
                    server.queue.push(json.dumps(request))
                    if to != 'public':
                        return

        for local_client in self.clients.values():  
            local_jid = local_client['jid']      
            if local_jid == to or to == 'public':
                if to != 'public' or request['from'] != local_jid:
                    self.replies[local_jid].append(request)
                if to != 'public':
                    return            

    async def _get_replies(self, request, websocket):    
        jid = request['to']
        replies = self.replies.pop(jid, [])
        self.replies[jid] = []
        await websocket.send(json.dumps(replies))

    async def _receive_message(self, message, websocket):  
        jid = message['to']
        if jid == 'public':
            for local_jid in self.replies:
                self.replies[local_jid].append(message)      
        else:
            if jid in self.replies:
                self.replies[jid].append(message)
            else:
                self.replies[jid] = [message]           
             
    def find_request_processor(self, request):
        if request['tag'] in self.processors:
            return self.processors[request['tag']]
        return None

    async def handle(self, request, websocket):
        request = json.loads(request)
        processor = self.find_request_processor(request)
        if processor is None:
            print("Unsupport request: " + json.dumps(request))
            return
        return await processor(request, websocket)   
        

    async def _check_alive(self, request, websocket): 
        await websocket.send(json.dumps({'tag': 'checked'}))

    async def _client_join(self, request, websocket):
        client = request['info']
        if client['jid'] in [c['jid'] for c in self.clients.values()]:
            return
        if not client['jid'].endswith('@' + self.config['localServer']['domain']):
            await websocket.send('Authentication failed!')
            await websocket.close()
            return
        
        await websocket.send('OK')
        self.clients[websocket.remote_address] = client
        self.replies[client['jid']] = []
        precense_message = await self._create_precense_message()
        await self._broadcast(precense_message)

    async def client_left(self, websocket):
        remote_address = websocket.remote_address
        is_client_left = remote_address in self.clients
        if is_client_left:
            self.clients.pop(websocket.remote_address)
            message = await self._create_precense_message()
            await self._broadcast(message)    

    async def _return_members(self, request, websocket):
        members = {}
        clients = []
        for c in self.clients.values():
            clients.append(c)
        members[f"local"] = clients    
        for server in self.servers.values():
            server_clients = []
            for m in server.members_info:
                server_clients.append(m)
            members[f"{server.ip_address}"] = server_clients     
        await websocket.send(json.dumps(members))     

    async def _create_precense_message(self):
        clients_info = [{"nickname": c['nickname'], 
                         "jid": c['jid'], 
                         "publickey": c['publickey']} for c in self.clients.values()]
        return json.dumps({
            "tag": "presence",
            "presence": clients_info
        })
    
    async def _broadcast(self, message):
        for server in self.servers.values():
            server.queue.push(message)

    async def _connect_server_success(self, request, websocket):
        await websocket.send(json.dumps(request))
        response = json.loads(await websocket.recv())
        return response['presence']