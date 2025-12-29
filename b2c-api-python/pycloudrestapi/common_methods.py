
import time
import re
from pycloudrestapi import constants
from pycloudrestapi.logger_config import logger

class CommonMethods:
    def __init__(self, debug):
        self.debug = debug

    def get_current_time(self):
        return f"{time.strftime('%H:%M:%S')} :"

    def string_is_null_or_empty(self, data):
        if data is None or data == "":
            return True
        return False

    def get_date_time(self):
        current_date = time.localtime()
        hr = str(current_date.tm_hour).zfill(2)
        minu = str(current_date.tm_min).zfill(2)
        sec = str(current_date.tm_sec).zfill(2)
        str_time = f"{hr}:{minu}:{sec}"
        return str_time

    def convert_to_decimal(self, stype, value, decimal_loc):
        return_val = 0
        nfixed = 4 if decimal_loc == 1000 else 2
        try:
            if value > 0:
                if stype == "Price":
                    return_val = round(value / decimal_loc, nfixed)
                elif stype == "Percentage":
                    return_val = value / decimal_loc
            else:
                return_val = value
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            return_val = "0"
        return return_val

    def get_time_in_seconds(self):
        seconds_since_epoch = round(time.time())
        return seconds_since_epoch

    def write_console_log(self, data):
        if self.debug:
            logger.error(f"{self.get_current_time()} {data}")

    def find_value(self, arr_input, str_check_for):
        sval = ""
        try:
            arr_input = "|" + arr_input
            my_reg_exp = re.compile(
                "[^0-9]" + str_check_for.strip() + "[ ]*=[ ]*([^|]*)")
            areg_result = my_reg_exp.search(arr_input)
            if areg_result is not None and areg_result.group(1) != "undefined" and areg_result.group(1) != "":
                sval = areg_result.group(1)
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
            raise error
        return sval

    def remove_field_delimiter(self, str_source):
        if str_source.endswith(constants.C_S_FIELD_DELIMITER):
            str_source = str_source[:-1]
        return str_source

    def look_up(self, str_response):
        lk_up = {}
        try:
            sfield_data = ""
            afield_data = None
            if str_response is not None:
                resp_data = str_response.split(constants.C_S_FIELD_DELIMITER)
                for _, sfield_data in enumerate(resp_data):
                    if sfield_data != "undefined":
                        afield_data = sfield_data.split(
                            constants.C_S_NAMEVALUE_DELIMITER)
                        lk_up[afield_data[0]] = afield_data[1]
        except Exception:
            pass
        return lk_up

    def get_price_formatter(self, str_decimal_locator, int_mkt_seg_id):
        try:
            if int_mkt_seg_id in [constants.C_V_MAPPED_MSX_DERIVATIVES, constants.C_V_MAPPED_MSX_SPOT, constants.C_V_MSX_DERIVATIVES, constants.C_V_MSX_SPOT, constants.C_V_NSX_DERIVATIVES, constants.C_V_NSX_SPOT, constants.C_V_MAPPED_NSX_DERIVATIVES, constants.C_V_MAPPED_NSX_SPOT, constants.C_V_BSECDX_DERIVATIVES, constants.C_V_BSECDX_SPOT, constants.C_V_MAPPED_BSECDX_DERIVATIVES, constants.C_V_MAPPED_BSECDX_SPOT, constants.C_V_MAPPED_MCX_DERIVATIVES]:
                if int_mkt_seg_id == constants.C_V_MAPPED_MCX_DERIVATIVES and str_decimal_locator.strip() == "0":
                    str_decimal_locator = "100"
                if str_decimal_locator.strip() == "0":
                    str_decimal_locator = "10000"
                if str_decimal_locator == "100":
                    return 2
                if str_decimal_locator in ["10000", constants.C_V_NSECDS_DECLOC]:
                    return 4
                return 2
            if int_mkt_seg_id in [constants.C_V_MAPPED_BFX_DERIVATIVES, constants.C_V_MAPPED_BFX_SPOT]:
                if str_decimal_locator == "1000":
                    return 3
                if str_decimal_locator == "10000":
                    return 4
                return 2
            if int_mkt_seg_id in [constants.C_V_MAPPED_MSX_CASH, constants.C_V_MAPPED_MSX_FAO]:
                return self.get_price_forma_from_dec_loc(int(str_decimal_locator))
            if str_decimal_locator is not None and str_decimal_locator != "" and str_decimal_locator != "0":
                return self.get_no_of_zeros(str_decimal_locator)
            return 2
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")

    def get_mapped_market_segment_id(self, imarket_segment_id):
        imapped_market_segment_id = -1
        try:
            if imarket_segment_id == constants.C_V_NSE_CASH:
                imapped_market_segment_id = constants.C_V_MAPPED_NSE_CASH
            elif imarket_segment_id == constants.C_V_NSE_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_NSE_DERIVATIVES
            elif imarket_segment_id == constants.C_V_BSE_CASH:
                imapped_market_segment_id = constants.C_V_MAPPED_BSE_CASH
            elif imarket_segment_id == constants.C_V_BSE_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_BSE_DERIVATIVES
            elif imarket_segment_id == constants.C_V_MCX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_MCX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_MCX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_MCX_SPOT
            elif imarket_segment_id == constants.C_V_NCDEX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_NCDEX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_NCDEX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_NCDEX_SPOT
            elif imarket_segment_id == constants.C_V_NSEL_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_NSEL_DERIVATIVES
            elif imarket_segment_id == constants.C_V_NSEL_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_NSEL_SPOT
            elif imarket_segment_id == constants.C_V_MSX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_MSX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_MSX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_MSX_SPOT
            elif imarket_segment_id == constants.C_V_NSX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_NSX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_NSX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_NSX_SPOT
            elif imarket_segment_id == constants.C_V_BSECDX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_BSECDX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_BSECDX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_BSECDX_SPOT
            elif imarket_segment_id == constants.C_V_MSX_CASH:
                imapped_market_segment_id = constants.C_V_MAPPED_MSX_CASH
            elif imarket_segment_id == constants.C_V_MSX_FAO:
                imapped_market_segment_id = constants.C_V_MAPPED_MSX_FAO
            elif imarket_segment_id == constants.C_V_NMCE_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_NMCE_DERIVATIVES
            elif imarket_segment_id == constants.C_V_DSE_CASH:
                imapped_market_segment_id = constants.C_V_MAPPED_DSE_CASH
            elif imarket_segment_id == constants.C_V_UCX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_UCX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_UCX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_UCX_SPOT
            elif imarket_segment_id == constants.C_V_DGCX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_DGCX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_DGCX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_DGCX_SPOT
            elif imarket_segment_id == constants.C_V_BFX_DERIVATIVES:
                imapped_market_segment_id = constants.C_V_MAPPED_BFX_DERIVATIVES
            elif imarket_segment_id == constants.C_V_BFX_SPOT:
                imapped_market_segment_id = constants.C_V_MAPPED_BFX_SPOT
            elif imarket_segment_id == constants.C_V_OFS_IPO_BONDS:
                imapped_market_segment_id = constants.C_V_MAPPED_OFS_IPO_BONDS
            else:
                imapped_market_segment_id = -1
        except Exception as error:
            if self.debug:
                logger.error(f"${error}")
        return imapped_market_segment_id

    def get_no_of_zeros(self, num):
        num = float(num)
        count = 0
        last = 0
        while last == 0:
            last = float(str(num % 10))
            num = num / 10
            count += 1
        return count - 1

    def parse_ter(self, int_mkt_seg_id, str_ter):
        if int_mkt_seg_id not in [constants.C_V_NSE_DERIVATIVES, constants.C_V_NSX_DERIVATIVES, constants.C_V_MAPPED_NSX_DERIVATIVES]:
            str_ter = ""
        return str_ter

    def get_price_forma_from_dec_loc(self, idecimal_lc):
        str_format = "2"
        try:
            str_format = str(len(str(idecimal_lc)) - 1)
        except Exception:
            pass
        return int(str_format)

    # def getPreciseValue(self, strOrgValue, MktSegId, DecimalLocator):
    #     strPrecValue = strOrgValue
    #     strPrecison = "2"  # Default Value as 2 precisions
    #     if strOrgValue != "" and float(strOrgValue) > 0:
    #         if MktSegId in [constants.C_V_MSX_DERIVATIVES, constants.C_V_MSX_SPOT, constants.C_V_NSX_SPOT, constants.C_V_NSX_DERIVATIVES, constants.C_V_BSECDX_SPOT, constants.C_V_BSECDX_DERIVATIVES]:
    #             if MktSegId in [constants.C_V_NSX_SPOT, constants.C_V_BSECDX_SPOT]:
    #                 if DecimalLocator == 100:
    #                     strPrecison = "2"
    #                 else:
    #                     strPrecison = "4"
    #             else:
    #                 strPrecison = "4"
    #         elif MktSegId in [constants.C_V_MCX_SPOT, constants.C_V_NSEL_SPOT, constants.C_V_MCX_DERIVATIVES]:
    #             if DecimalLocator == 10000:
    #                 strPrecison = "4"
    #         elif MktSegId in [constants.C_V_MAPPED_BFX_DERIVATIVES, constants.C_V_MAPPED_BFX_SPOT, constants.C_V_BFX_DERIVATIVES, constants.C_V_BFX_SPOT]:
    #             if DecimalLocator == 1000:
    #                 strPrecison = "3"
    #             if DecimalLocator == 10000:
    #                 strPrecison = "4"
    #         elif MktSegId in [constants.C_V_MSX_CASH, constants.C_V_MSX_FAO, constants.C_V_MAPPED_MSX_CASH, constants.C_V_MAPPED_MSX_FAO]:
    #             strFormat = "2"
    #             strFormat = str(len(str(DecimalLocator)) - 1)
    #             strPrecison = strFormat
    #         strPrecValue = round(float(strOrgValue), int(strPrecison))
    #     return strPrecValue

    def get_date_time_part(self, str_dt):
        if self.trim(str_dt) != "":
            if " " not in str_dt:
                return str_dt
            str_dt = str_dt.split(" ")
            if len(str_dt) == 2:
                if float(str_dt[1]) == 0:
                    return ""
                str1 = self.trim(str_dt[1])[:2]
                str2 = self.trim(str_dt[1])[2:4]
                str3 = self.trim(str_dt[1])[4:6]
                return f"{str1}:{str2}:{str3} {str_dt[0]}"
            return ""
        return ""

    def trim(self, obj_txt_value):
        return str(obj_txt_value).replace(" ", "")
