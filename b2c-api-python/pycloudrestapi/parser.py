
from pycloudrestapi import constants
import pycloudrestapi.common_methods
from pycloudrestapi.common_methods import CommonMethods as common_methods
from pycloudrestapi.logger_config import logger

class APIParser:
    def __init__(self, debug):
        self.mask_msg_length = "#PACKET_LENGTH#"
        self.debug = debug
        self.common_methods = common_methods(debug=self.debug)

    def create_request_message_header(self, message_code):
        try:
            sb_message_header = ""
            message_code = str(message_code)
            current_time = self.common_methods.get_date_time()
            sb_message_header += constants.C_S_TAG_MSGVERSION + constants.C_S_NAMEVALUE_DELIMITER + \
                constants.C_S_TAG_COMMPROTOCOL + constants.C_S_FIELD_DELIMITER
            sb_message_header += constants.C_S_TAG_MSGCODE + \
                constants.C_S_NAMEVALUE_DELIMITER + message_code + constants.C_S_FIELD_DELIMITER
            sb_message_header += constants.C_S_TAG_MSGLENGTH + constants.C_S_NAMEVALUE_DELIMITER + \
                self.mask_msg_length + constants.C_S_FIELD_DELIMITER
            sb_message_header += constants.C_S_TAG_MSGTIME + \
                constants.C_S_NAMEVALUE_DELIMITER + current_time + constants.C_S_FIELD_DELIMITER

            return sb_message_header

        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            pass

    def calculate_message_length(self, _request):
        try:
            int_pkt_length = len(_request)
            str_pkt_length = str(
                int_pkt_length + len(str(int_pkt_length)) - len(self.mask_msg_length))
            return _request.replace(self.mask_msg_length, str_pkt_length)

        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            pass

    def create_login_request(self, reqLogin):
        try:
            sb_final_request = ""
            sb_final_request += self.create_request_message_header(
                constants.C_V_MSGCODES_LOGINREQUEST)
            sb_final_request += constants.C_S_TAG_USERID + constants.C_S_NAMEVALUE_DELIMITER + \
                reqLogin['userId'] + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_PWD + \
                constants.C_S_NAMEVALUE_DELIMITER + constants.C_S_FIELD_DELIMITER
            # _sbFinalRequest += constants.C_S_TAG_PWD + constants.C_S_NAMEVALUE_DELIMITER + reqLogin.token + constants.C_S_FIELD_DELIMITER # TODO
            sb_final_request += constants.C_S_CONNECTIONTYPE + \
                constants.C_S_NAMEVALUE_DELIMITER + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_V_TAG_IWINLOGONTAG + constants.C_S_NAMEVALUE_DELIMITER + \
                constants.C_S_NETNETLOGONTAG + constants.C_S_FIELD_DELIMITER
            # _sbFinalRequest += constants.C_V_TAG_PWD_TYPE + constants.C_S_NAMEVALUE_DELIMITER + constants.C_S_ENCYP_TYPE_TOKEN + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_V_TAG_PWD_TYPE + \
                constants.C_S_NAMEVALUE_DELIMITER + "1" + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_GROUPID + \
                constants.C_S_NAMEVALUE_DELIMITER + "HO" + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_CONNECTION_TYPE + constants.C_S_NAMEVALUE_DELIMITER + \
                constants.C_S_CONNECTIONTYPE + constants.C_S_FIELD_DELIMITER
            # _sbFinalRequest += constants.C_V_TAG_TENANTID + constants.C_S_NAMEVALUE_DELIMITER + reqLogin.tenantId + constants.C_S_FIELD_DELIMITER
            # _sbFinalRequest += "395=127.0.0.1"
            str_final_request = self.calculate_message_length(
                sb_final_request)
            return str_final_request

        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            self.common_methods.write_console_log(
                "Error in createLoginRequest: " + error)

    def create_touch_line_request(self, operation_type, lst_tl_request, full_ltp_req):
        try:
            sb_final_request = ""
            sb_final_request += self.create_request_message_header(
                constants.C_S_MSGCODE_MULTIPLE_TOUCHLINE_REQUEST)
            # sbFinalRequest += constants.C_S_TAG_SESSIONID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.User.OCToken + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_SESSIONID + \
                constants.C_S_NAMEVALUE_DELIMITER + constants.C_S_FIELD_DELIMITER  # TODO
            # sbFinalRequest += constants.C_V_TAG_TENANTID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.ComId + constants.C_S_FIELD_DELIMITER
            for _, tl_request in enumerate(lst_tl_request):
                sb_final_request += constants.C_S_TAG_MKTSEGID + \
                    constants.C_S_NAMEVALUE_DELIMITER + \
                    tl_request.get("MktSegId")
                sb_final_request += constants.C_S_RECORD_DELIMITER
                sb_final_request += constants.C_S_TAG_SCRIPTOKEN + \
                    constants.C_S_NAMEVALUE_DELIMITER + tl_request.get("token")
                sb_final_request += constants.C_S_FIELD_DELIMITER
            if not full_ltp_req:
                sb_final_request += constants.C_S_TAG_CUMULATIVEOI + \
                    constants.C_S_NAMEVALUE_DELIMITER + "1" + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_OPERATIONTYPE + \
                constants.C_S_NAMEVALUE_DELIMITER + str(operation_type)
            # sbFinalRequest += constants.C_S_FIELD_DELIMITER
            # sbFinalRequest += constants.C_V_TAG_SI_TEMPLATEID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.User.SITemplateId
            final_request = self.calculate_message_length(sb_final_request)
            return final_request
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            pass

    def create_best_five_request(self, obj_best_five_req):
        try:
            sb_final_request = ""
            sb_final_request += self.create_request_message_header(
                constants.C_S_MSGCODE_BESTFIVE_REQUEST)
            # sbFinalRequest += constants.C_S_TAG_SESSIONID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.User.OCToken + constants.C_S_FIELD_DELIMITER
            # sbFinalRequest += constants.C_V_TAG_TENANTID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.ComId + constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_SCRIPTOKEN + \
                constants.C_S_NAMEVALUE_DELIMITER + \
                obj_best_five_req.get("token")
            sb_final_request += constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_MKTSEGID + \
                constants.C_S_NAMEVALUE_DELIMITER + \
                obj_best_five_req.get("MktSegId")
            sb_final_request += constants.C_S_FIELD_DELIMITER
            sb_final_request += constants.C_S_TAG_OPERATIONTYPE + \
                constants.C_S_NAMEVALUE_DELIMITER + \
                str(obj_best_five_req.get("operation"))
            # sbFinalRequest += constants.C_S_FIELD_DELIMITER
            # sbFinalRequest += constants.C_V_TAG_SI_TEMPLATEID + constants.C_S_NAMEVALUE_DELIMITER + clsGlobal.User.SITemplateId
            str_final_request = self.calculate_message_length(sb_final_request)
            return str_final_request
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            pass

    def process_logon_response(self, response_packet):
        obj_resp = {}
        str_login_response = ""
        smsg_code = ""
        sauth_code = ""
        try:
            smsg_code = self.common_methods.find_value(
                response_packet, constants.C_S_TAG_MSGCODE)
            res_array = self.common_methods.look_up(response_packet)
            sauth_code = res_array[constants.C_S_TAG_AUTHCODE]
            if sauth_code != "":
                obj_resp["MsgCode"] = smsg_code
                obj_resp["MsgData"] = "Login Success"
                obj_resp["MsgTime"] = self.common_methods.get_current_time()
                obj_resp["MsgCategory"] = constants.C_S_MSGCAT_ACK
            return obj_resp
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            pass

    def process_multi_touch_line_resp(self, response_packet):
        obj_multi_tl_response = {}
        try:
            lst_response = self.common_methods.look_up(response_packet)
            sk_scrip = {}
            sk_scrip["MktSegId"] = int(
                lst_response[constants.C_S_TAG_MKTSEGID])
            map_mkt_seg_id = self.common_methods.get_mapped_market_segment_id(
                sk_scrip["MktSegId"])
            sk_scrip["token"] = lst_response[constants.C_S_TAG_SCRIPTOKEN]
            # _objMultiTLResponse = clsResponseStore.getTouchLineResponseObject(skScrip.toString(), true)
            # if response received for scrip which is removed from request store then return null
            if obj_multi_tl_response is None:
                return obj_multi_tl_response

            obj_multi_tl_response["Scrip"] = sk_scrip
            obj_multi_tl_response["LUT"] = lst_response[constants.C_S_TAG_LUT]

            int_decimal_locator = 100

            if lst_response[constants.C_S_TAG_DECIMALLOCATOR] == "" or lst_response[constants.C_S_TAG_DECIMALLOCATOR] == "0":
                int_decimal_locator = 100
            else:
                int_decimal_locator = int(
                    lst_response[constants.C_S_TAG_DECIMALLOCATOR])

            str_price_format = self.common_methods.get_price_formatter(
                str(int_decimal_locator), map_mkt_seg_id)

            # _objMultiTLResponse["PriceFormat"] = _strPriceFormat
            obj_multi_tl_response["DecimalLocator"] = int_decimal_locator
            # these tags will always be present in touchline Response whether CCast On or OFF so directly accessing value from list
            obj_multi_tl_response["LTP"] = self.price_formatter(
                lst_response[constants.C_S_TAG_LTP], int_decimal_locator, str_price_format)
            if obj_multi_tl_response["LTP"] == "":
                obj_multi_tl_response["LTP"] = "0.00"

            obj_multi_tl_response["PercNetChange"] = "{:.2f}".format(
                float(lst_response[constants.C_S_TAG_NETCHANGEFROMPREVCLOSE]))

            obj_multi_tl_response["LTQ"] = str(
                self.qty_formatter(lst_response[constants.C_S_TAG_LTQ], 0))
            obj_multi_tl_response["LTT"] = lst_response[constants.C_S_TAG_LTT]

            obj_multi_tl_response["BuyQty"] = str(
                self.qty_formatter(lst_response[constants.C_S_TAG_BUYQTY], 0))
            obj_multi_tl_response["BuyPrice"] = self.price_formatter(
                lst_response[constants.C_S_TAG_BUYPRICE], int_decimal_locator, str_price_format)

            obj_multi_tl_response["SellQty"] = str(
                self.qty_formatter(lst_response[constants.C_S_TAG_SELLQTY], 0))
            obj_multi_tl_response["SellPrice"] = self.price_formatter(
                lst_response[constants.C_S_TAG_SELLPRICE], int_decimal_locator, str_price_format)

            obj_multi_tl_response["Volume"] = str(
                self.qty_formatter(lst_response[constants.C_S_TAG_VOLUME], 0))
            obj_multi_tl_response["ClosePrice"] = self.price_formatter(
                lst_response[constants.C_S_TAG_CLOSEPRICE], int_decimal_locator, str_price_format)
            obj_multi_tl_response["ATP"] = self.price_formatter(
                lst_response[constants.C_S_TAG_ATP], int_decimal_locator, str_price_format)

            if constants.C_S_TAG_NETCHANGEINRS in lst_response:
                obj_multi_tl_response["NetChangeInRs"] = lst_response[constants.C_S_TAG_NETCHANGEINRS]

            if constants.C_S_TAG_OPENPRICE in lst_response:
                obj_multi_tl_response["OpenPrice"] = self.price_formatter(
                    lst_response[constants.C_S_TAG_OPENPRICE], int_decimal_locator, str_price_format)

            if constants.C_S_TAG_HIGHPRICE in lst_response:
                obj_multi_tl_response["HighPrice"] = self.price_formatter(
                    lst_response[constants.C_S_TAG_HIGHPRICE], int_decimal_locator, str_price_format)

            if constants.C_S_TAG_LOWPRICE in lst_response:
                obj_multi_tl_response["LowPrice"] = self.price_formatter(
                    lst_response[constants.C_S_TAG_LOWPRICE], int_decimal_locator, str_price_format)

            if constants.C_S_TAG_OPENINTEREST in lst_response:
                obj_multi_tl_response["OpenInt"] = lst_response[constants.C_S_TAG_OPENINTEREST]

            if constants.C_S_TAG_TOTBUYQTY in lst_response:
                # changed to double as for index token in TotBuyQty MktCapitalization will be received
                obj_multi_tl_response["TotBuyQty"] = str(
                    self.qty_formatter(lst_response[constants.C_S_TAG_TOTBUYQTY], 1))

            if constants.C_S_TAG_TOTSELLQTY in lst_response:
                obj_multi_tl_response["TotSellQty"] = str(
                    self.qty_formatter(lst_response[constants.C_S_TAG_TOTSELLQTY], 0))

            if constants.C_S_TAG_LIFETIMEHIGH in lst_response:
                obj_multi_tl_response["LifeTimeHigh"] = "{:.{}f}".format(float(
                    lst_response[constants.C_S_TAG_LIFETIMEHIGH]) / int_decimal_locator, str_price_format)

            if constants.C_S_TAG_LIFETIMELOW in lst_response:
                obj_multi_tl_response["LifeTimeLow"] = "{:.{}f}".format(float(
                    lst_response[constants.C_S_TAG_LIFETIMELOW]) / int_decimal_locator, str_price_format)

            if constants.C_S_TAG_DPR in lst_response:
                obj_multi_tl_response["DPR"] = lst_response[constants.C_S_TAG_DPR]

            if constants.C_S_TAG_PERC_OPENINTEREST in lst_response:
                obj_multi_tl_response["PercOpenInt"] = lst_response[constants.C_S_TAG_PERC_OPENINTEREST]

            if constants.C_S_TAG_HIGH_OPENINTEREST in lst_response:
                obj_multi_tl_response["HighOpenInt"] = lst_response[constants.C_S_TAG_HIGH_OPENINTEREST]

            if constants.C_S_TAG_LOW_OPENINTEREST in lst_response:
                obj_multi_tl_response["LowOpenInt"] = lst_response[constants.C_S_TAG_LOW_OPENINTEREST]

            if constants.C_S_TAG_TRADE_EXECUTION_RANGE in lst_response:
                obj_multi_tl_response["TER"] = self.common_methods.parse_ter(
                    sk_scrip["MktSegId"], lst_response[constants.C_S_TAG_TRADE_EXECUTION_RANGE])

            lst_response = None
            sk_scrip = None
            response_packet = None

            return obj_multi_tl_response
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")

    def process_best_five_response(self, response_packet):
        obj_best_five_response = None
        try:
            str_resp_data = []
            il_resp = self.common_methods.look_up(response_packet)
            str_segment_id = il_resp[constants.C_S_TAG_MKTSEGID]
            str_token = il_resp[constants.C_S_TAG_SCRIPTOKEN]

            obj_scrip = {}
            obj_scrip["MktSegId"] = int(
                str_segment_id) if str_segment_id != "" else -1
            map_mkt_seg_id = self.common_methods.get_mapped_market_segment_id(
                obj_scrip["MktSegId"])
            obj_scrip["token"] = str_token

            # gets the Best5 object (new or existing) from response store based on Scrip key
            obj_best_five_response = {}

            obj_best_five_response["Scrip"] = obj_scrip
            int_decimal_locator = 100
            dec_locator = 100
            if il_resp[constants.C_S_TAG_DECIMALLOCATOR] == "" or il_resp[constants.C_S_TAG_DECIMALLOCATOR] == "0" or il_resp[constants.C_S_TAG_DECIMALLOCATOR] == "100.00":
                int_decimal_locator = 100
            else:
                # Used decimal variable to handle the case of MCXSX where the Decimal locator was "10000.00"
                dec_locator = float(il_resp[constants.C_S_TAG_DECIMALLOCATOR])
                int_decimal_locator = dec_locator
            str_price_format = self.common_methods.get_price_formatter(str(
                int_decimal_locator), map_mkt_seg_id)  # intDecimalLocator.toString().length - 1;

            str_volume = il_resp[constants.C_S_TAG_VOLUME]
            str_open_price = il_resp[constants.C_S_TAG_OPENPRICE]
            str_close_price = il_resp[constants.C_S_TAG_CLOSEPRICE]

            str_per_chg = il_resp[constants.C_S_TAG_NETCHANGEFROMPREVCLOSE]
            str_tbq = il_resp[constants.C_S_TAG_TOTBUYQTY]
            str_dpr = il_resp[constants.C_S_TAG_DPR]
            str_ltt = il_resp[constants.C_S_TAG_LTT]
            str_lut = il_resp[constants.C_S_TAG_LUT]

            str_high_price = il_resp[constants.C_S_TAG_HIGHPRICE]
            str_low_price = il_resp[constants.C_S_TAG_LOWPRICE]
            str_ltq = il_resp[constants.C_S_TAG_LTQ]
            str_ltp = il_resp[constants.C_S_TAG_LTP]

            str_year_high_price = il_resp[constants.C_S_TAG_LIFETIMEHIGH]
            str_year_low_price = il_resp[constants.C_S_TAG_LIFETIMELOW]

            str_tsq = il_resp[constants.C_S_TAG_TOTSELLQTY]
            str_atp = il_resp[constants.C_S_TAG_ATP]

            response_packet = response_packet.replace("|12", "&12")
            str_resp_data = response_packet.split("|")

            b5 = ""
            s5 = ""
            for i, data in enumerate(str_resp_data):
                if len(data) > 0:
                    if data.find("&") == -1:
                        # don"t know what has been done here; find out later
                        # let sdata = data.split("=")
                        continue
                    else:
                        if data.startswith("11=1"):
                            b5 = data
                        if data.startswith("11=2"):
                            s5 = data

            # First Split entire Buy and Sell data with respect to &
            # We will have length of array equal to 6
            str_buy_data = b5.split("&")
            str_sell_data = s5.split("&")

            # Iterate through above Buy and Sell data arrays
            # starting from 1st index, since 0th index has BUY and Sell indication only
            # NOTE: Here Length of strBuyData and strSellData will be same hence use any one array length
            str_b1 = ""
            str_s1 = ""
            obj_best_five_response["BestFiveData"] = [{}, {}, {}, {}, {}]
            for i in range(1, len(str_buy_data)):
                # Split individual data w.r.t. $
                if str_buy_data[i] is not None:
                    str_b1 = str_buy_data[i].split("$")
                if str_sell_data[i] is not None:
                    str_s1 = str_sell_data[i].split("$")

                dic_buy_data = {}
                dic_sell_data = {}
                # Further Split Buy data record w.r.t "=" to segregate No of Buyers, Price, Quantity
                for _, value in enumerate(str_b1):
                    sdata_b = value.split("=")
                    dic_buy_data[sdata_b[0]] = sdata_b[1]

                # Further Split Sell data record w.r.t "=" to segregate No of Sellers, Price, Quantity
                for _, s in enumerate(str_s1):
                    sdata_s = s.split("=")
                    dic_sell_data[sdata_s[0]] = sdata_s[1]

                # Fill properties of B5DetailData (Buy and Sell data together as single row)
                # Normal case
                obj_best_five_response["BestFiveData"][i - 1]["sBid"] = self.price_formatter(
                    dic_buy_data[constants.C_S_TAG_ORDPRICE], int_decimal_locator, str_price_format)
                # self.common_methods.getPreciseValue(
                #     self.priceFormatter(dicBuyData[constants.C_S_TAG_ORDPRICE], intDecimalLocator, _strPriceFormat),
                #     int(strSegmentId),
                #     intDecimalLocator
                # ) if dicBuyData[constants.C_S_TAG_ORDPRICE] != "" and float(dicBuyData[constants.C_S_TAG_ORDPRICE]) > 0 else "-"
                obj_best_five_response["BestFiveData"][i -
                                                       1]["sBidQty"] = dic_buy_data[constants.C_S_TAG_ORIGQTY] if dic_buy_data[constants.C_S_TAG_ORIGQTY] != "" else "-"
                obj_best_five_response["BestFiveData"][i -
                                                       1]["sBuyers"] = dic_buy_data[constants.C_S_TAG_BEST5NOOFORDERS] if dic_buy_data[constants.C_S_TAG_BEST5NOOFORDERS] != "" else "-"
                obj_best_five_response["BestFiveData"][i - 1]["sAsk"] = self.price_formatter(
                    dic_sell_data[constants.C_S_TAG_ORDPRICE], int_decimal_locator, str_price_format)
                # self.common_methods.getPreciseValue(
                #     self.priceFormatter(dicBuyData[constants.C_S_TAG_ORDPRICE], intDecimalLocator, _strPriceFormat),
                #     int(strSegmentId),
                #     intDecimalLocator
                # ) if dicSellData[constants.C_S_TAG_ORDPRICE] != "" and float(dicSellData[constants.C_S_TAG_ORDPRICE]) > 0 else "-"
                obj_best_five_response["BestFiveData"][i -
                                                       1]["sAskQty"] = dic_sell_data[constants.C_S_TAG_ORIGQTY] if dic_sell_data[constants.C_S_TAG_ORIGQTY] != "" else "-"
                obj_best_five_response["BestFiveData"][i -
                                                       1]["sSellers"] = dic_sell_data[constants.C_S_TAG_BEST5NOOFORDERS] if dic_sell_data[constants.C_S_TAG_BEST5NOOFORDERS] != "" else "-"

                # Clear the dictionary to facilitate its filling on the next iteration
                dic_buy_data = None
                dic_sell_data = None

            obj_best_five_response["DecimalLocator"] = int_decimal_locator
            obj_best_five_response["LUT"] = str_lut
            obj_best_five_response["Volume"] = str_volume if str_volume != "" and int(
                str_volume) > 0 else ""

            # Normal Case
            if not obj_best_five_response.get("IsSpread"):
                obj_best_five_response["OpenPrice"] = self.price_formatter(
                    str_open_price, int_decimal_locator, str_price_format) if str_open_price != "" and float(str_open_price) > 0 else ""
                obj_best_five_response["ClosePrice"] = self.price_formatter(
                    str_close_price, int_decimal_locator, str_price_format) if str_close_price != "" and float(str_close_price) > 0 else ""
                obj_best_five_response["HighPrice"] = self.price_formatter(
                    str_high_price, int_decimal_locator, str_price_format) if str_high_price != "" and float(str_high_price) > 0 else ""
                obj_best_five_response["LowPrice"] = self.price_formatter(
                    str_low_price, int_decimal_locator, str_price_format) if str_low_price != "" and float(str_low_price) > 0 else ""
                obj_best_five_response["LTP"] = self.price_formatter(
                    str_ltp, int_decimal_locator, str_price_format) if str_ltp != "" and float(str_ltp) > 0 else ""
            else:
                obj_best_five_response["OpenPrice"] = str_open_price if str_open_price != "" and str_open_price != "0" else ""
                obj_best_five_response["ClosePrice"] = str_close_price if str_close_price != "" and str_close_price != "0" else ""
                obj_best_five_response["HighPrice"] = str_high_price if str_high_price != "" and str_high_price != "0" else ""
                obj_best_five_response["LowPrice"] = str_low_price if str_low_price != "" and str_low_price != "0" else ""
                obj_best_five_response["LTP"] = str_ltp if str_ltp != "" and str_ltp != "0" else ""

            # Condition handled for preopen
            if self.common_methods.trim(str_per_chg) == "-100" or self.common_methods.trim(str_per_chg) == "-100.00":
                str_per_chg = ""
            obj_best_five_response["PercNetChange"] = str_per_chg
            obj_best_five_response["TotBuyQty"] = str_tbq if str_tbq != "" and int(
                str_tbq) > 0 else ""
            obj_best_five_response["DPR"] = str_dpr
            obj_best_five_response["LTT"] = self.common_methods.get_date_time_part(str_ltt)
            obj_best_five_response["LUT"] = self.common_methods.get_date_time_part(str_lut)

            obj_best_five_response["LTQ"] = str_ltq if str_ltq != "" and int(
                str_ltq) > 0 else ""
            # If LTP is present then display Percentage change else blank
            obj_best_five_response["PercNetChange"] = str_per_chg if obj_best_five_response["LTP"] != "" else ""
            obj_best_five_response["YrHighPrice"] = self.price_formatter(
                str_year_high_price, int_decimal_locator, str_price_format) if str_year_high_price != "" and float(str_year_high_price) > 0 else ""
            obj_best_five_response["YrLowPrice"] = self.price_formatter(
                str_year_low_price, int_decimal_locator, str_price_format) if str_year_low_price != "" and float(str_year_low_price) > 0 else ""

            obj_best_five_response["TotSellQty"] = str_tsq if str_tsq != "" and int(
                str_tsq) > 0 else ""
            obj_best_five_response["ATP"] = self.price_formatter(
                str_atp, int_decimal_locator, str_price_format) if str_atp != "" and float(str_atp) > 0 else ""

            if constants.C_S_TAG_TRADE_EXECUTION_RANGE in il_resp:
                obj_best_five_response["TER"] = self.common_methods.parse_ter(
                    obj_scrip["MktSegId"], il_resp.get(constants.C_S_TAG_TRADE_EXECUTION_RANGE))
            # _objBestFiveResponse.TER = self.common_methods.ParseTER(objScrip.ScripDet.MktSegId, iLResp[constants.C_S_TAG_TRADE_EXECUTION_RANGE])

            if constants.C_S_TAG_PERC_OPENINTEREST in il_resp:
                obj_best_five_response["OI"] = il_resp[constants.C_V_TAG_OPENINTEREST]

            if constants.C_S_TAG_PERC_OPENINTEREST in il_resp:
                obj_best_five_response["PercOpenInt"] = il_resp[constants.C_S_TAG_PERC_OPENINTEREST]

            if constants.C_S_TAG_HIGH_OPENINTEREST in il_resp:
                obj_best_five_response["HighOpenInt"] = il_resp[constants.C_S_TAG_HIGH_OPENINTEREST]

            if constants.C_S_TAG_LOW_OPENINTEREST in il_resp:
                obj_best_five_response["LowOpenInt"] = il_resp[constants.C_S_TAG_LOW_OPENINTEREST]

            net_change_in_rs = 0
            if obj_best_five_response["LTP"] != "" and obj_best_five_response["ClosePrice"] != "":
                net_change_in_rs = float(
                    obj_best_five_response["LTP"]) - float(obj_best_five_response["ClosePrice"])

            obj_best_five_response["NetChangeInRs"] = net_change_in_rs if net_change_in_rs != "" else ""
            obj_best_five_response["PriceFormat"] = str_price_format

            il_resp = None
            obj_scrip = None
            response_packet = None
            str_resp_data = None

            return obj_best_five_response

        except Exception as error:
            if self.debug:
                logger.error(f"${error}")

    def price_formatter(self, sprice_val, int_dec_loc, str_price_format):
        dec_price = float(sprice_val)
        return str(format((dec_price / int_dec_loc), f".{str_price_format}f")) if dec_price > 0 else str("0.00")

    def qty_formatter(self, sqty_val, int_value_in):
        if int_value_in == 0:
            return str(int(sqty_val)) if int(sqty_val) > 0 else str(0)
        if int_value_in == 1:
            return str(float(sqty_val)) if float(sqty_val) > 0 else str(0)
