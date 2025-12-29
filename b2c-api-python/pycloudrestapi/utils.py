from datetime import datetime

mapping_msg_soc_resp = {
    "UniqueCode": {
        "property": "order_id"
    },
    "Exchange": {
        "property": "exchange",
        "values": {
            '1': "NSE_EQ",
            '2': "NSE_FO",
            '3': "BSE_EQ",
            '4': "BSE_FO",
            '5': "MCX_FO",
            '6': "MCX_SPOT",
            '7': "NCDEX_FO",
            '8': "NCDEX_SPOT",
            '9': "NSEL_SPTCOM",
            #// 10: "",
            '11': "MSE_CUR",
            '12': "MSE_SPOT",
            '13': "NSE_CUR",
            '14': "NSECUR_SPOT",
            '15': "MSE_EQ",
            '16': "MSE_FO",
            #// 17: "",
            '18': "BSE_COMM",
            '19': "NSE_COMM",
            '20': "NSECOMM_SPOT",
            #// 21: "",
            #// 25: "",
            #// 26: "",
            #// 27: "",
            #// 31: "",
            #// 32: "",
            '33': "NSE_OTS",
            '34': "ICEX_FO",
            '36': "",
            #// 37: "",
            '38': "BSE_CUR",
            '39': "BSECUR_SPOT",
            '40': "BSECOMM_SPOT",
            '100': "",
            '521': "EQ Combined",
            '4102': "FNO Combined",
            '268438528': "CDS Combined",

        }
    },
    "ScripCode": {
        "property": "scrip_token",
    },
    "OrderNumber": {
        "property": "exchange_order_no"
    },
    "OrderStatus": {
        "property": "status",
        "values": {
            "-1": "NOT_INITIATED",
            "1": "CLIENT_XMITTED",
            "2": "GATEWAY_XMITTED",
            "3": "OMS_XMITTED",
            "4": "EXCHANGE_XMITTED",
            "5": "PENDING",
            "6": "CANCELLED",
            "7": "EXECUTED",
            "8": "ADMIN_PENDING",
            "9": "GATEWAY_REJECT",
            "10": "OMS_REJECT",
            "11": "ORDER_ERROR",
            "12": "FROZEN",
            "13": "M.PENDING",
            "14": "ADMIN_ACCEPT",
            "15": "ADMIN_REJECT",
            "16": "ADMIN_MODIFY",
            "17": "ADMIN_CANCEL",
            "18": "AMO_SUBMITTED",
            "19": "AMO_CANCELLED",
            "20": "COMPLETED",
            "21": "STOPPED",
            "22": "CONVERTED",
        }
    },
    "Reason": {
        "property": "error_reason",
    },
    "Buy_Sell": {
        "property": "transaction_type",
        "values": {
            '1': "BUY",
            '2': "SELL"
        }
    },
    "Product": {
        "property": "product_type",
        "values": {
            "D": "DELIVERY",
            "M": "INTRADAY",
            "MF": "MTF",
            "PT": "BTST",
            "MP": "COVER",
            "B": "BRACKET",
            "AD": "DELIVERY",
            "AM": "INTRADAY",
        }
    },
    "OrderType": {
        "property": "order_type",
        "values": {
            "1": "RL",
            "2": "RL-MKT",
            "3": "SL",
            "4": "SL-MKT",
        }
    },
    "OrderOriginalQty": {
        "property": "total_quantity"
    },
    "PendingQty": {
        "property": "pending_quantity"
    },
    "TradedQTY": {
        "property": "traded_quantity"
    },
    "DQ": {
        "property": "disclosed_quantity"
    },
    "OrderPrice": {
        "property": "order_price"
    },
    "TriggerPrice": {
        "property": "trigger_price"
    },
    "OrderValidity": {
        "property": "validity",
    },
    "Days": {
        "property": "validity_days",
    },
    "Symbol": {
        "property": "symbol"
    },
    "Series": {
        "property": "series"
    },
    "InstrumentName": {
        "property": "instrument"
    },
    "ExpiryDate": {
        "property": "expiry_date"
    },
    "StrikePrice": {
        "property": "strike_price"
    },
    "Option_Type": {
        "property": "option_type"
    },
    "OrderEntryTime": {
        "property": "order_timestamp"
    },
    "LastModifiedTime": {
        "property": "exchange_timestamp"
    },
    "InitiatedBy": {
        "property": "initiated_by"
    },
    "ModifiedBy": {
        "property": "modified_by"
    },
    "UserRemarks": {
        "property": "order_identifier"
    }
}

conditions = {
    "OrderValidity": lambda mapped_value, source_obj: "EOSESS" if source_obj["Exchange"] == 2 and mapped_value == "1" else "EOTODY" if source_obj["Exchange"] == 2 and mapped_value == "2" else "EOSTLM" if source_obj["Exchange"] == 2 and mapped_value == "3" else "IOC" if source_obj["Exchange"] == "2" and mapped_value == "4" else "DAY" if mapped_value == "1" else "GTD" if mapped_value == "2" else "GTC" if mapped_value == "3" else "IOC" if mapped_value == "4" else "EOS" if mapped_value == "5" else "FOK" if mapped_value == "6" else mapped_value,
    "OrderType": lambda mapped_value, source_obj: source_obj["Exchange"] == 5 or source_obj["Exchange"] == 6 and "RL" or "CA" if not mapped_value and source_obj["OrderType"] == 11 else "RL" if source_obj["OrderType"] == 12 and (source_obj["Exchange"] == 11 or source_obj["Exchange"] == 12 or source_obj["Exchange"] == 15 or source_obj["Exchange"] == 16) else mapped_value,
    "OrderEntryTime": lambda mapped_value, source_obj: datetime.strptime(mapped_value, "%d-%b-%Y %H.%M.%S").strftime("%Y-%m-%d %H.%M.%S") if mapped_value else "",
    "LastModifiedTime": lambda mapped_value, source_obj: datetime.strptime(mapped_value, "%d-%b-%Y %H.%M.%S").strftime("%Y-%m-%d %H.%M.%S") if mapped_value else "",
}

def map_resp(source_obj):
    result_obj = {}
    for key, mapping in mapping_msg_soc_resp.items():
        if key in source_obj:
            mapped_value = mapping["values"][str(source_obj[key])] if "values" in mapping else source_obj[key]
            if key in conditions:
                mapped_value = conditions[key](mapped_value, source_obj)
            result_obj[mapping["property"]] = mapped_value
    return result_obj

def mapped_msg_soc_resp(source_obj):
    mapped_resp = map_resp(source_obj)
    mapped_resp.update({
        'message_type': source_obj['MessageType'],
        'is_amo_order': True if source_obj['Product'] in ['AD', 'AM'] else False,
    })
    return mapped_resp