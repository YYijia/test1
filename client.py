import asyncio
import websockets
import sys
import os
import json
import time
import threading
from business import Queue, Member
from cryptography.hazmat.primitives import hashes   
from cryptography.hazmat.primitives.asymmetric import padding, rsa  
from cryptography.hazmat.primitives import serialization 
import base64
import yaml

def generate_pair_keys():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)  
    public_key = private_key.public_key()
    return private_key, public_key

def to_member(m):
    return Member(m['jid'], m['nickname'], None, serialization.load_pem_public_key(m['publickey'].encode()))

async def encrypt(public_key, message):
    if type(message) is str:
        message = message.encode()
    return base64.b64encode(public_key.encrypt(message, encrypter)).decode()

async def decrypt(private_key, cipher_text, to_string=True):
    content = private_key.decrypt(base64.b64decode(cipher_text.encode()), decypter)
    return content.decode() if to_string else content

async def join(websocket):
    global client
    public_key_pem = client.public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo)
    message = {
        'tag': 'join',
        'info': {
            'nickname': client.nickname,
            'jid': client.jid,
            'publickey': public_key_pem.decode()
        }
    }
    await websocket.send(json.dumps(message))
    response = await websocket.recv()
    return 1 if response == 'OK' else 0

async def get_replies(websocket):
    global client
    message = {
        'tag': 'get_replies',
        'to': client.jid
    }
    await websocket.send(json.dumps(message))
    messages = json.loads(await websocket.recv())
    for message in messages:
        if message['tag'] == 'message':
            message['info'] = await decrypt(client.private_key, message['info'])
        elif message['tag'] == 'file':
            file_name = message['filename']
            file_content = await decrypt(client.private_key, message['info'], to_string=False)
            with open(file_name, 'wb') as file:
                file.write(file_content)
            message['info'] = f'You received a file {file_name}'     
          
    return messages    
    
async def get_members(websocket):    
    global all_members
    message = {'tag': 'members'}
    await websocket.send(json.dumps(message))
    members = json.loads(await websocket.recv())
    members_list = {}
    for server_members in members.values():
        for m in server_members:
            members_list[m['jid']] = to_member(m)
    with lock:
        all_members = members_list        

async def send_message(target, content, websocket):    
    global client, all_members
    message = {
        'tag': 'send_message',
        'from': client.jid,
        'to': target,
        'info': await encrypt(all_members[target].public_key, content)
    }
    await websocket.send(json.dumps(message))

async def send_file(target, file_path, websocket):
    global client, all_members
    with open(file_path, 'rb') as file:
        file_content = file.read()
    with lock:    
        public_key = all_members[target].public_key
    file_content = await encrypt(public_key, file_content)    
    message = {
        'tag': 'send_file',
        'from': client.jid,
        'to': target,
        'filename': os.path.basename(file_path),
        'info': file_content
    }
    
    await websocket.send(json.dumps(message))    

async def connect():
    global all_members, replies, queue, connected
    with open('config.yaml') as file:
        config = yaml.safe_load(file)
    uri = f"ws://{config['localServer']['ipAddress']}:{config['localServer']['port']}"
    # we will update members information each 1 seconds
    time_update_members = time.time() + 1
    try:
        async with websockets.connect(uri) as websocket:
            connected = await join(websocket)
            while connected == 1:
                if time.time() >= time_update_members:
                    await get_members(websocket)
                    time_update_members = time.time() + 1
                message_replies = await get_replies(websocket)
                
                if len(message_replies) > 0:
                    with lock:
                        replies.extend(message_replies)
                if not queue.is_empty():
                    target, content, isFile = queue.pop() 
                    if isFile:
                        await send_file(target, content, websocket)     
                    else:
                        await send_message(target, content, websocket)     
                await asyncio.sleep(0.2)
    except Exception as ex:
        if type(ex) == websockets.ConnectionClosedOK:
            print('Server closed connection!')
            exit(0)
        else:
            print(f'Error: {type(ex).__name__}')         


client = None
all_members = []
queue = Queue()
replies = []
lock = threading.Lock()
encrypter = padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None)  
decypter = padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),  algorithm=hashes.SHA256(), label=None)  
connected = -1 # -1: connection initialize, 0: connect failed, 1: connect successful, 2: connection closed

def connect_server():
    global client
    private_key, public_key = generate_pair_keys()
    client = Member(jid = sys.argv[1], 
                    nickname = sys.argv[2] if len(sys.argv) > 2 else '',
                    private_key = private_key,
                    public_key = public_key)
    asyncio.run(connect())

def main():
    global queue, replies, connected
    while True:
        if connected == 0:
            print('Authentication failed!')
            return
        if connected == 1:
            break
        time.sleep(0.2)

    print("Command(1 -> View members, 2 -> Chat, 3 -> Transfer file, 4 -> View messages, 5 -> Exit)")
    while connected == 1:
        command = int(input('Enter your command: '))
        if command == 1:
            with lock:
                for m in all_members.values():
                    print("jid: " + m.jid + ", nickname: " + m.nickname)
        elif command == 4:
            with lock:
                if len(replies) > 0:
                    for reply in replies:
                        print(f"Message from {reply['from']}: {reply['info']}")
                    replies = []        
        elif command == 2:
            message = input('Input with format jid:content > ')
            if len(message.strip()) > 0:
                target = message[:message.index(':')]
                with lock:
                    if target not in all_members:
                        print(f'{target} is either offline or not exist!')
                        continue
                content = message[message.index(':') + 1:].strip()   
                queue.push((target, content, None))
        elif command == 3:
            message = input('Input with format jid:file path > ')
            if len(message.strip()) > 0:
                target = message[:message.index(':')]
                content = message[message.index(':') + 1:].strip()   
                queue.push((target, content, True))   
        elif command == 5:
            connected = 2             

if __name__ == "__main__":
    t = threading.Thread(target=connect_server)
    t.start()
    main()
