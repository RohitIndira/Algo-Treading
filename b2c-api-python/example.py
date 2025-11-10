import asyncio
import pyotp
from pycloudrestapi import IBTConnect

async def main():
    API_URL = "<<Trading API Endpoint>>"
    API_KEY = "<<API Key>>"
    USER_ID = "<<User id>>"
    PASSWORD = "<<Password>>"
    TOTP = pyotp.TOTP("<<User TOTP Server key>>").now()

    ibt_connect = IBTConnect(params={
        "baseurl": API_URL,
        "api_key": API_KEY,
        "debug": True
    })

    logon_response = ibt_connect.login(params={
        "userId": USER_ID,
        "password": PASSWORD,
        "totp": TOTP
    })
    # print(f"Login response : ${logon_response}")

    if logon_response.get("data") is not None:

        # broadcast socket
        async def on_open_broadcast_socket(message):
            print('broadcast socket: open', message, "\n\n")
            await ibt_connect.touchline_subscription([{ "MktSegId": '1', "token": '22' }, { "MktSegId": '1', "token": '2885' }])  # add market type
            await ibt_connect.bestfive_subscription({"MktSegId": "1", "token": "22"})
        
        async def on_close_broadcast_socket(close_msg):
            print("broadcast socket: close", close_msg, "\n\n")

        async def on_error_broadcast_socket(error):
            print("broadcast socket: error", error, "\n\n")

        async def on_touchline(message):
            print("broadcast socket: touchline", message, "\n\n")

        async def on_bestfive(message):
            print("broadcast socket: bestfive", message, "\n\n")

        # Assign the callbacks.
        ibt_connect.on_open_broadcast_socket = on_open_broadcast_socket
        ibt_connect.on_close_broadcast_socket = on_close_broadcast_socket
        ibt_connect.on_error_broadcast_socket = on_error_broadcast_socket
        ibt_connect.on_touchline = on_touchline
        ibt_connect.on_bestfive = on_bestfive

        # message socket
        async def on_ready_message_socket(response):
            print("message socket:", response, "\n\n")

        async def on_close_message_socket(close_msg):
            print("message socket: close", close_msg)

        async def on_error_message_socket(error):
            print("message socket: error", error)

        async def on_msg_message_socket(response):
            print("message socket: data:", response, "\n\n")

        ibt_connect.on_ready_message_socket = on_ready_message_socket
        ibt_connect.on_close_message_socket = on_close_message_socket
        ibt_connect.on_msg_message_socket = on_msg_message_socket

        ibt_connect.on_error_message_socket = on_error_message_socket
        
        # orderParams = {
        #     "scrip_info": {
        #         "exchange": "NSE_EQ",
        #         "scrip_token": 15124,
        #         "symbol": "KOLTEPATIL",
        #         "series": "EQ",
        #         "expiry_date": "",
        #         "strike_price": "",
        #         "option_type": ""
        #     },
        #     "transaction_type": "BUY",
        #     "product_type": "INTRADAY",
        #     "order_type": "RL",
        #     "quantity": 10,
        #     "price": 300,
        #     "trigger_price": 0,
        #     "disclosed_quantity": 0,
        #     "validity": "DAY",
        #     "validity_days": 0,
        #     "is_amo": False,
        #     "order_identifier": "",
        #     "part_code": "",
        #     "algo_id": "",
        #     "strategy_id": "",
        #     "vender_code": ""
        # }

        # orderResp = ibt_connect.place_order(orderParams)
        # print("Place normal order api response : ", orderResp, "\n\n")

        # orderbookResp = ibt_connect.get_order_book({})
        # print("Order book api response : ", orderbookResp, "\n\n")

        # tradebookResp = ibt_connect.get_trade_book({})
        # print("Trade book api response : ", tradebookResp, "\n\n")

        # orderId = orderResp.get("data")
        # orderHistoryResp = ibt_connect.get_order_history(orderId)
        # print("Order history api response : ", orderHistoryResp, "\n\n")

        await asyncio.gather(
            ibt_connect.connect_broadcast_socket(),
            ibt_connect.connect_message_socket(),
        )

asyncio.run(main())