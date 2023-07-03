from algo_trade.market.strategy.analysis.option_chain import GenericOptionChain
from algo_trade.utils import AsyncLoggingMeta


class StockOptionChain(GenericOptionChain, metaclass=AsyncLoggingMeta):
    def get_select_stock_option_chain(
            self, symbol: str, expiry: str, strike: int = None
            ):
        expiries = self.get_all_monthly_expiries()
        all_symbols = self.get_fo_list()
        
        if symbol in all_symbols and expiry in expiries:
            data, _ = self.get_option_chain(symbol)
            
            return data
        
        return None
    
    def get_select_stock_options_bhavcopy(self, symbol: str, expiry: str):
        
        expiry_set: list = self.get_all_monthly_expiries()
        fo_symbols: list = self.get_fo_list()
        ce_data, pe_data = self.get_stock_options_bhavcopy()
        
        if expiry in expiry_set and symbol in fo_symbols:
            ce_data = ce_data.loc[
                      (ce_data.Symbol == symbol)&(
                              ce_data.Expiry_dt == expiry), :
                      ]
            pe_data = pe_data.loc[
                      (pe_data.Symbol == symbol)&(
                              pe_data.Expiry_dt == expiry), :
                      ]
            
            return ce_data.round(2), pe_data.round(2)
        
        return None
    
    def stock_option_chain(self,
                           symbol: str,
                           delta: int = 5,
                           expiry_delta: int = 1) -> str:
        """
        Stock Option Chain Analysis method.
        :param symbol:
        :param delta:
        :param expiry_delta:
        :return:
        """
        
        # TODO: Identify ATM Strike based on the underlying value.
        # TODO:
        
        option_chain, expiry = obj.get_option_chain(symbol, "equity", expiry_delta)
        
        underlying_value = option_chain["underlying_value"]
        oc_df = option_chain["OptionChain"]
        
        return option_chain


if __name__ == '__main__':
    obj = StockOptionChain()
    
    print(option_chain)
