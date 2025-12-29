
# import configparser
import logging
import zlib
import requests
import asyncio 
import websockets
from pycloudrestapi import constants
from pycloudrestapi.common_methods import CommonMethods as common_methods
from pycloudrestapi.parser import APIParser as parser
from pycloudrestapi import utils
from pycloudrestapi.logger_config import logger
import socketio

log = logging.getLogger(__name__)

class IBTConnect:
    timeout = 10000
    source = "MOBILEAPI"
    second_auth_type = "TOTP"
    login_type = "PASSWORD"
    logon_response = ""
    is_connected = False
    bcast_header_len = 6
    bcast_old_data = None
    msg_code_key_to_check = constants.C_S_FIELD_DELIMITER + \
        constants.C_S_TAG_MSGCODE + constants.C_S_NAMEVALUE_DELIMITER

    routes = {
        "session": "/authentication/v1/user/session",
        "balance": "/authentication/v1/user/balance",

        "placeOrder": "/transactional/v1/orders/regular",
        "modifyOrders": "/transactional/v1/orders/regular/{exchange}/{order_id}",

        "coverOrder": "/transactional/v1/orders/cover",
        "modifyCoverOrder": "/transactional/v1/orders/cover/{exchange}/{order_id}",

        "bracketOrder": "/transactional/v1/orders/bracket",
        "modifyBracketOrder": "/transactional/v1/orders/bracket/{exchange}/{order_id}",

        "placeMultilegOrder": "/transactional/v1/orders/multileg",
        "cancelMultilegOrder": "/transactional/v1/orders/multileg/{order_flag}/{gateway_order_no}", 

        "orders": "/transactional/v1/orders",
        "trades": "/transactional/v1/trades",
        "orderHistory": "/transactional/v1/orders/{order_id}",

        "positions": "/transactional/v1/portfolio/positions/{type}",
        "positionConvertion": "/transactional/v1/portfolio/positions",
        "holdings": "/transactional/v1/portfolio/holdings",
    }

    def __init__(self, params):

        self.base_url = params.get("baseurl")
        self.api_key = params.get("api_key")
        self.x_api_key = params.get("x-api-key")
        self.debug = params.get("debug") or False
        # self.proxies = proxies if proxies else {}
        # Create requests session by default
        # Same session to be used by pool connections
        self.reqsession = requests.Session()

        # disable requests SSL warning
        # requests.packages.urllib3.disable_warnings() # Module "requests.packages" has no "urllib3"
        # Initialize the APIParser
        self.parser = parser(debug=self.debug)
        self.common_methods = common_methods(debug=self.debug)

        # callbacks
        self.on_open_broadcast_socket = None
        self.on_close_broadcast_socket = None
        self.on_error_broadcast_socket = None
        self.on_touchline = None
        self.on_bestfive = None

        self.on_ready_message_socket = None
        self.on_close_message_socket = None
        self.on_error_message_socket = None
        self.on_msg_message_socket = None

    def request(self, method, url, path_var=None, query_params=None, body=None, is_json=True):

        url = self.base_url + url
        # if method in ["GET", "DELETE"] and query_params:
        #     URL = url + "?" + query_params

        if url.find("{") != -1:
            for key in path_var:
                url = url.replace("{" + key + "}", path_var.get(key))

        headers = {
            "x-api-key": self.x_api_key
        }
        if self.logon_response and self.logon_response["access_token"]:
            headers["Authorization"] = "Bearer " + \
                self.logon_response["access_token"]

        if self.debug:
            logger.info(f"Request: {method} {url} {body} {headers}")

        try:
            response = self.reqsession.request(method,
                                               url,
                                               headers=headers,
                                               json=body if (
                                                   method in ["POST", "PUT"] and is_json) else None,
                                               data=body if (
                                                   method in ["POST", "PUT"] and not is_json) else None,
                                               params=query_params,
                                               # verify=not self.disable_ssl,
                                               allow_redirects=True,
                                               timeout=self.timeout)  # , proxies=self.proxies)
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            raise error

        if self.debug:
            logger.info(f"Response: {response.status_code} {response.content}")

        if "json" in response.headers["content-type"]:
            try:
                data = response.json()
            except ValueError as errvalue:
                raise errvalue

            return data

    def login(self, params):

        data = self.request("POST", self.routes["session"], body={
            "user_id": params.get("userId"),
            "login_type": self.login_type,
            "password": params.get("password"),
            "second_auth_type": self.second_auth_type,
            "second_auth": params.get("totp"),
            "api_key": self.api_key,
            "source": self.source
        })

        if data and "data" in data:
            self.logon_response = data["data"]

        return data
    
    def balance(self):
        data = self.request("GET", self.routes["balance"])
        return data

    def validateSession(self):
        data = self.request("PUT", self.routes["session"])
        return data
    
    def logout(self):
        data = self.request("DELETE", self.routes["session"])
        return data
    
    def get_order_book(self, params):

        data = self.request("GET", self.routes["orders"], query_params={
            "offset": params.get("offset") or "1",
            "limit": params.get("limit") or "20",
            "orderStatus": params.get("orderStatus") or None,
            "order_id": params.get("order_id") or None
        })

        return data

    def get_trade_book(self, params):

        data = self.request("GET", self.routes["trades"], query_params={
            "offset": params.get("offset") or "1",
            "limit": params.get("limit") or "20"
        })

        return data

    def get_order_history(self, params):

        data = self.request("GET", self.routes["orderHistory"], path_var={
            "order_id": params.get("orderId"),
        })

        return data

    def place_order(self, params):

        data = self.request("POST", self.routes["placeOrder"], body=params)

        return data

    def modify_order(self, params):

        data = self.request("PUT", self.routes["modifyOrders"], path_var={
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        }, body=params)

        return data

    def cancel_order(self, params):

        data = self.request("DELETE", self.routes["modifyOrders"], path_var={
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        })

        return data
    
    def place_cover_order(self, params):
        data = self.request("POST", self.routes["coverOrder"], body=params)
        return data
    
    def modify_cover_order(self, params):
        path_var = {
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        }
        data = self.request("PUT", self.routes["modifyCoverOrder"], path_var=path_var, body=params)
        return data

    def cancel_cover_order(self, params):
        path_var = {
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        }
        data = self.request("DELETE", self.routes["modifyCoverOrder"], path_var=path_var)
        return data

    def place_bracket_order(self, params):
        data = self.request("POST", self.routes["bracketOrder"], body=params)
        return data

    def modify_bracket_order(self, params):
        path_var = {
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        }
        data = self.request("PUT", self.routes["modifyBracketOrder"], path_var=path_var, body=params)
        return data

    def delete_bracket_order(self, params):
        path_var = {
            "exchange": params.get("exchange"),
            "order_id": params.get("order_id"),
        }
        data = self.request("DELETE", self.routes["modifyCoverOrder"], path_var=path_var)
        return data
    
    def place_multileg_order(self, params):
        data = self.request("POST", self.routes["placeMultilegOrder"], body=params)
        return data

    def cancel_multileg_order(self, params):
        path_var = {
            "order_flag": params.get("order_flag"),
            "gateway_order_no": params.get("gateway_order_no"),
        }
        data = self.request("PUT", self.routes["cancel_multileg_order"], path_var=path_var, body=params)
        return data

    def get_positions(self, params):
        path_var = {
            "type": params.get("type"),
        }
        data = self.request("GET", self.routes["positions"], path_var=path_var)
        return data
    
    def position_conversion(self, params):
        data = self.request("PUT", self.routes["positionConvertion"], body=params)
        return data

    def get_holdings(self):
        data = self.request("GET", self.routes["holdings"])
        return data
    
    # message socket
    async def connect_message_socket(self):
        self.sio = socketio.AsyncClient()

        @self.sio.event
        async def connect():
            await self.sio.emit('loginAPI', {'jToken': self.logon_response['access_token']})
            if self.on_ready_message_socket:
                return await self.on_ready_message_socket('open')

        @self.sio.event
        async def disconnect():
            if self.on_close_message_socket:
                return await self.on_close_message_socket('Message socket disconnected')
        
        @self.sio.on('MSG:DATA')
        async def message(data):
            if 'MessageType' in data:
                mapped_response = utils.mapped_msg_soc_resp(data)
                if self.on_msg_message_socket:
                    return await self.on_msg_message_socket(mapped_response)
            else:
                if self.on_msg_message_socket:
                    return await self.on_msg_message_socket(data)
        try:
            await self.sio.connect(self.logon_response['others']['messageSocket'], transports=['websocket'])
            await self.sio.wait()
        except Exception as error:
            if self.debug:
                logger.error(f"message socket: ${error}")

    # broadcast socket
    async def connect_broadcast_socket(self):
        uri = self.logon_response["others"]["broadCastSocket"] if self.logon_response else "wss://odindemo.63moons.com:4510",
        try:
            async with websockets.connect(uri[0]) as websocket:
                self.bcast_socket = websocket
                self.is_connected = True
                await self.send_login()
                async for message in websocket:
                    await self.on_message(message)
                if self.debug:
                    logger.info("Broadcast websocket: Connection opened")
        except websockets.exceptions.ConnectionClosed:
            close_msg = "Broadcast websocket: Connection closed"
            await self.on_close(close_msg)
        except websockets.exceptions.WebSocketException as error:
            await self.on_error()

    async def send_login(self):
        objLogin = {
            "userId": self.logon_response["user_id"],
            "token": self.logon_response["access_token"]
        }
        slogin_request = self.parser.create_login_request(objLogin)
        await self.send_message(slogin_request)

    async def send_message(self, request):
        data_to_send = self.fragment_data(request)
        await self.bcast_socket.send(data_to_send)

    async def on_message(self, message):
        await self.process_packet(bytearray(message))

    async def on_error(self, error):
        if self.debug:
            logger.error(f"Broadcast websocket: error {error}")
        if self.on_error_broadcast_socket:
            return await self.on_error_broadcast_socket(error)

    async def on_close(self, close_msg):
        
        if self.on_close_broadcast_socket:
            return await self.on_close_broadcast_socket(close_msg)

    async def send_touchline_req(self, req_tl):
        try:
            tl_req = self.parser.create_touch_line_request(req_tl.get(
                "operation"), req_tl.get("arrScrip"), True)
            if self.is_connected:
                await self.send_message(tl_req)
            else:
                async def delayed_send():
                    await self.send_message(tl_req)
                asyncio.get_event_loop().call_later(0.5, delayed_send)
        except Exception as error:
            if self.debug:
                logger.error(f"send_touchline_req: ${error}")

    async def send_best_five_req(self, req_b5):
        try:
            b5_req = self.parser.create_best_five_request(req_b5)
            if self.is_connected:
                await self.send_message(b5_req)
            else:
                async def delayed_send():
                    await self.send_message(b5_req)
                asyncio.get_event_loop().call_later(0.5, delayed_send)
        except Exception as error:
            if self.debug:
                logger.error(f"send_best_five_req: ${error}")

    def fragment_data(self, request):
        ba_request = self.handle_compressed_data(request)
        length = len(ba_request) + 4
        length_string = str(length).rjust(5, "0")
        len_bytes = self.string_to_binary(length_string)

        ba_actual_send = []
        ba_actual_send.append(5)
        ba_actual_send[1:] = len_bytes
        ba_actual_send[6:] = ba_request

        ba_actual_send.append(0)
        ba_actual_send.append(0)
        ba_actual_send.append(0)
        ba_actual_send.append(0)

        # TODO: AJAY
        # _baActualSend[76] = 176
        # _baActualSend[77] = 21
        # _baActualSend[78] = 84
        # _baActualSend[79] = 186

        return bytearray(ba_actual_send)

    def handle_compressed_data(self, raw_data):
        comp_data = zlib.compress(self.string_to_binary(raw_data), 7)  # , 6)
        return bytearray(comp_data)

    def string_to_binary(self, data):
        binary_list = []
        # Iterate through each character in the string
        for char in data:
            binary_list.append(ord(char) & 0xFF)
        return bytearray(binary_list)

    # Process socket responses
    async def process_packet(self, message):
        try:
            full_msg = self.defrag_packet(bytearray(message))
            if full_msg is not None and len(full_msg) > 0:
                while len(full_msg) > 0:
                    msg = full_msg.pop()
                    _response = "".join(chr(c) for c in msg)
                    if "|50=" not in _response:
                        int_tmtr_index = _response.find(constants.C_S_CHAR0)
                        if int_tmtr_index != -1:
                            _response = _response[:int_tmtr_index]
                    arr_data = []
                    if "|50=" not in _response:
                        arr_data = _response.split(constants.C_S_CHAR2)
                    else:
                        arr_data = self.parse_message(_response)
                    int_data_count = len(arr_data)
                    for int_data_cntr in range(int_data_count):
                        if arr_data[int_data_cntr] != "":
                            await self.process_packet_string(arr_data[int_data_cntr])
        except Exception as error:
            err_msg = "WebSocket Client Message Receive Error: " + str(error)
            if self.debug:
                logger.error(f"process_packet: ${err_msg}")

    def defrag_packet(self, data):
        if len(data) == 0:
            return None

        full_messages = []
        data_received = self.append_or_copy_buffer(self.bcast_old_data, data)

        is_done = True
        msg_length = 0
        while is_done:
            if len(data_received) < self.bcast_header_len:
                self.bcast_old_data = data_received
                return full_messages

            msg_length = self.get_message_length(data_received)
            if msg_length <= 0:
                return None

            if len(data_received) < (msg_length + self.bcast_header_len):
                self.bcast_old_data = data_received
                return full_messages
            else:
                self.bcast_old_data = None

            compressed_message = bytearray(
                data_received[self.bcast_header_len:msg_length + self.bcast_header_len])
            uncompressed_byte_message = self.get_uncompressed_message(
                compressed_message)
            if uncompressed_byte_message is None:
                is_done = False
                break

            full_messages.append(uncompressed_byte_message)

            if len(data_received) == (msg_length + self.bcast_header_len):
                is_done = False
                compressed_message = None
                break

            new_message = bytearray(
                data_received[msg_length + self.bcast_header_len:])
            data_received = new_message

        data_received = None
        data = None

        return full_messages

    def append_or_copy_buffer(self, buffer1, buffer2):
        if (buffer1):
            tmp = bytearray(len(buffer1) + len(buffer2))
            tmp[:len(buffer1)] = buffer1
            tmp[len(buffer1):] = buffer2
            return tmp
        else:
            tmp = bytearray(len(buffer2))
            tmp[:len(buffer2)] = buffer2
            return tmp

    def get_message_length(self, message):
        if message[0] == 5:
            self.is_uncompress = False
        else:
            self.is_uncompress = True
        try:
            str_packet_length = "".join(chr(byte)
                                        for byte in message[1:self.bcast_header_len])
            return int(str_packet_length)
        except:
            return 0

    def get_uncompressed_message(self, compressed_message):
        uncompressed_bytes = None
        if not self.is_uncompress:
            uncompressed_bytes = bytearray(zlib.decompress(compressed_message))
        else:
            uncompressed_bytes = bytearray(compressed_message)
        return uncompressed_bytes

    def parse_message(self, str_message):
        try:
            arr_msg = []
            if str_message is not None:
                index = 0
                while True:
                    str_msg_len = str_message[0:6]
                    msg_len = int(str_msg_len[1:])
                    index += 6
                    str_new_msg = str_message[index:index + msg_len]
                    # print("New Message: ", strNewMsg)
                    str_message = str_message[(msg_len + 6):]
                    # if strMessage.length > 0:
                    #     # print("Rem Message: ", strMessage)
                    # index += msgLen
                    index = 0
                    # strMessage = strMessage.substr(index, msgLen)
                    arr_msg.append(str_new_msg)
                    if str_message == "" or str_message is None:
                        break
            return arr_msg

        except Exception as e:
            if self.debug:
                logger.error(f"Error parsing message: ${e}")
            return []

    async def process_packet_string(self, response_packet):
        try:
            str_message_code = ""
            bsend_to_parser = False
            if self.msg_code_key_to_check in response_packet:
                str_message_code = self.common_methods.find_value(
                    response_packet, constants.C_S_TAG_MSGCODE)
                bsend_to_parser = True

            if bsend_to_parser:
                response_packet = self.common_methods.remove_field_delimiter(
                    response_packet)

                # TODO: AJAY
                if str_message_code == constants.C_S_MSGCODE_SOCKETLOGONRESPONSE:
                    obj_chnl_log_resp = self.parser.process_logon_response(response_packet)
                    if self.on_open_broadcast_socket:
                        return await self.on_open_broadcast_socket( { "data": obj_chnl_log_resp } )
                elif str_message_code == constants.C_S_MSGCODE_MULTIPLE_TOUCHLINE_RESPONSE:
                    obj_multi_tl_resp = self.parser.process_multi_touch_line_resp(response_packet)
                    if self.on_touchline:
                        return await self.on_touchline( { "data": obj_multi_tl_resp } )
                    # self.eventEmitter.emit(constants.EVT_SCK_TOUCHLINE, {"data": _objMultiTLResp})
                elif str_message_code == constants.C_S_MSGCODE_BESTFIVE_RESPONSE:
                    obj_b5_response = self.parser.process_best_five_response(response_packet)
                    if self.on_bestfive:
                        return await self.on_bestfive( { "data": obj_b5_response } )
                    # self.eventEmitter.emit(constants.EVT_SCK_MKTDEPTH, {"data": objB5Response})
                # elif strMessageCode == constants.C_S_TAG_TRADE_EXECUTION_RANGE_RESPONSE:
                    # objTERResponse = self.parser.processTERResponse(_responsePacket)
                # elif strMessageCode == constants.C_S_MSGCODE_LTP_MULTIPLE_TOUCHLINE_RESPONSE:
                #     if self.on_touchline:
                #         return self.on_touchline(_objMultiTLResp)
                    # _objLTPMultiTLResp = self.parser.processLTPMultiTouchLineResponse(_responsePacket)
                    # self.eventEmitter.emit(constants.EVT_SCK_TOUCHLINE, {"data": _responsePacket})
                # elif strMessageCode == constants.C_S_MSGCODE_MTL_MULTIPLE_TOUCHLINE_RESPONSE:
                #     if self.on_touchline:
                #         return self.on_touchline(_objMultiTLResp)
                    # _objMultiMTLResp = self.parser.processMultiMTLResponse(_responsePacket)
                    # self.eventEmitter.emit(constants.EVT_SCK_TOUCHLINE, {"data": _responsePacket})
                # elif strMessageCode == constants.C_S_MSGCODE_APP_HEARTBEAT_RESPONSE:
                #     pass
                else:
                    pass
        except Exception as error:
            if self.debug:
                logger.error(f"process_packet_string: ${error}")
            pass

    async def touchline_subscription(self, lst_scrip):
        if not isinstance(lst_scrip, list) or len(lst_scrip) <= 0:
            return {"data": lst_scrip, "message": "Invalid request"}
        req_tl = {"arrScrip": lst_scrip, "operation": 1}
        await self.send_touchline_req(req_tl)

    async def touchline_unsubscription(self, lst_scrip):
        if not isinstance(lst_scrip, list) or len(lst_scrip) <= 0:
            return {"data": lst_scrip, "message": "Invalid request"}
        req_tl = {"arrScrip": lst_scrip, "operation": 2}
        await self.send_touchline_req(req_tl)

    async def bestfive_subscription(self, params_obj):
        req_b5 = {**params_obj, "operation": 1}
        await self.send_best_five_req(req_b5)

    async def bestfive_unsubscription(self, params_obj):
        req_b5 = {**params_obj, "operation": 2}
        await self.send_best_five_req(req_b5)
