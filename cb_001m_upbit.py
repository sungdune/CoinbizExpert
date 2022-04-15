import pyupbit
import os
import pandas as pd
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
from talib import abstract


# n개월치 데이터 가져오기 - 재활용 가능한 함수로 구현
def get_ohlcv_dump(tickers= ['KRW-BTC'], filename = 'data.pickle', workdate = datetime.now(), reqmonths = 7, force_download = False):
    """    download ohlcv data from upbit and dump it as a pickle. if already has one, use it.
    Parameters    
        filename(String) : pickle name
        workdate(String, format %Y-%m-%d) : base date of work, download until workdate-1
        reqmonths(int) : num of requiring month
        force_download(bool)
    returns
        ohlcvas(key : ticker, value : pandas dataframe)

    """
    ohlcvs = {}
    if force_download or not os.path.exists(filename):
        
        startdate = workdate - relativedelta(months=reqmonths)
        
        for ticker in tickers:
            ohlcvs[ticker] = pyupbit.get_ohlcv_from(ticker, interval="minute1", to=workdate.strftime("%Y%m%d"), fromDatetime=startdate)
            # just for fast test, get 3 rows
            # ohlcvs[ticker] = pyupbit.get_ohlcv(ticker, interval="minute1", to=workdate.strftime("%Y%m%d"), count = 3)
        
        # Save pickle
        with open(filename,"wb") as fw:
            pickle.dump(ohlcvs, fw)
        
    else:
        # Load pickle
        with open(filename,"rb") as fr:
            ohlcvs = pickle.load(fr)

    return ohlcvs
    

def create_features(df, tp = 10, multiple = 3, repeat = 6):

    ######### 항목 생성 시작
    for _ in range(repeat): #6
        
        # 공통항목
        df[['upperband' + str(tp),'middleband' + str(tp),'lowerband' + str(tp)]] \
                                        = abstract.BBANDS(df, timeperiod = tp, nbdevup=2.0, nbdevdn=2.0)
        df['upperband' + str(tp)] = df['upperband' + str(tp)] / df['close']
        # df['middleband' + str(tp)] = df['middleband' + str(tp)] / df['close']
        # df['lowerband' + str(tp)] = df['lowerband' + str(tp)] / df['close']
        # df['dema' + str(tp)]     = abstract.DEMA(df, timeperiod = tp) / df['close']
        df['ema' + str(tp)]      = abstract.EMA(df, timeperiod = tp) / df['close']
        df['kama' + str(tp)]     = abstract.KAMA(df, timeperiod = tp) / df['close']
        # df['midpoint' + str(tp)] = abstract.MIDPOINT(df, timeperiod=tp) / df['close']
        df['midprice' + str(tp)] = abstract.MIDPRICE(df, timeperiod=tp) / df['close']
        # df['t3' + str(tp)]       = abstract.T3(df, timeperiod=tp, vfactor=0.7) / df['close']
        df['tema' + str(tp)]     = abstract.TEMA(df, timeperiod=tp) / df['close']
        df['trima' + str(tp)]    = abstract.TRIMA(df, timeperiod=tp) / df['close']
        df['wma' + str(tp)]      = abstract.WMA(df, timeperiod=tp) / df['close']
        
        # 모멘텀
        # df['adx' + str(tp)]      = abstract.ADX(df, timeperiod=tp) # no normal
        df['adxr' + str(tp)]     = abstract.ADXR(df, timeperiod=tp) # no normal
        # df['apo' + str(tp)]      = abstract.APO(df, fastperiod=tp, slowperiod = int(tp/2)) / df['close']
        # df[['aroonup' + str(tp),'aroondown' + str(tp)]] \
                                        # = abstract.AROON(df, timeperiod=tp) # no normal
        df['aroonosc' + str(tp)] = abstract.AROONOSC(df, timeperiod=tp) # no normal
        # df['cci' + str(tp)]      = abstract.CCI(df, timeperiod=tp) # no normal
        df['cmo' + str(tp)]      = abstract.CMO(df, timeperiod=tp) # no normal
        df['dx' + str(tp)]       = abstract.DX(df, timeperiod=tp) # no normal
        df[['macd' + str(tp),'macds' + str(tp),'macdh' + str(tp)]] \
                                        = abstract.MACD(df, fastperiod=tp, slowperiod = int(tp/2), signalperiod = int(tp/3)) # no normal
        df['mfi' + str(tp)]      = abstract.MFI(df, timeperiod=tp) # no normal
        df['minus_di' + str(tp)] = abstract.MINUS_DI(df, timeperiod=tp) # no normal
        df['minus_dm' + str(tp)] = abstract.MINUS_DM(df, timeperiod=tp) # no normal
        # df['mom' + str(tp)]      = abstract.MOM(df, timeperiod=tp) # no normal
        df['plus_di' + str(tp)]  = abstract.PLUS_DI(df, timeperiod=tp) # no normal
        df['plus_dm' + str(tp)]  = abstract.PLUS_DM(df, timeperiod=tp) # no normal
        df['ppo' + str(tp)]      = abstract.PPO(df, fastperiod=tp, slowperiod = int(tp/2)) # no normal
        # df['roc' + str(tp)]      = abstract.ROC(df, timeperiod=tp) # no normal
        # df['rsi' + str(tp)]      = abstract.RSI(df, timeperiod=tp) # no normal
        df[['slowk' + str(tp),'slowd' + str(tp)]] \
                                        = abstract.STOCH(df, fastk_period=tp, slowk_period= int(tp/2), slowd_period= int(tp/2)) # no normal
        df[['fastk' + str(tp),'fastd' + str(tp)]] \
                                        = abstract.STOCHF(df, fastk_period=tp, fastd_period= int(tp/2)) # no normal
        # df[['fastkrsi' + str(tp),'fastdrsi' + str(tp)]] \
                                        # = abstract.STOCHRSI(df, timeperiod=tp, fastk_period=tp, fastd_period= int(tp/2)) # no normal
        df['trix' + str(tp)]     = abstract.TRIX(df, timeperiod=tp) # no normal
        # df['ultosc' + str(tp)]   = abstract.ULTOSC(df, timeperiod1=int(tp/4), timeperiod2=int(tp/2), timeperiod3=tp) # no normal
        # df['willr' + str(tp)]    = abstract.WILLR(df, timeperiod=tp) # no normal

        # Volume        
        df['adosc' + str(tp)]    = abstract.ADOSC(df, fastperiod=int(tp/3), slowperiod=tp) # no normal?

        # Volatility 
        df['natr' + str(tp)]               = abstract.NATR(df, timeperiod=tp) # no normal

        # stat
        df['reg_inter' + str(tp)]          = abstract.LINEARREG_INTERCEPT(df, timeperiod=tp) / df['close']
        df['reg_slope' + str(tp)]          = abstract.LINEARREG_SLOPE(df, timeperiod=tp) # no normal
        
        tp *= multiple
    
    # 공통항목
    df[['mama','fama']]  = abstract.MAMA(df, fastlimit=0.5, slowlimit=0.05)
    df['mama'] = df['mama'] / df['close']
    df['fama'] = df['fama'] / df['close']
    df['ht']             = abstract.HT_TRENDLINE(df) / df['close']
    df['sar']            = abstract.SAR(df, acceleration=0.02, maximum=0.2) / df['close']
    
    # 모멘텀
    df['bop']            = abstract.BOP(df) # no normal

    # Volume
    df['ad']             = abstract.AD(df) # no normal?
    df['obv']            = abstract.OBV(df) # no normal?

    # Volatility 
    df['trange']         = abstract.TRANGE(df) / df['close']

    # Cycle 
    df['ht_dcperiod']    = abstract.HT_DCPERIOD(df) # no normal?
    df['ht_dcphase']     = abstract.HT_DCPHASE(df) # no normal?
    df[['ht_inphase','ht_quad']]            = abstract.HT_PHASOR(df) # no normal?
    df[['ht_sine','ht_leadsine']]           = abstract.HT_SINE(df) # no normal?
    df['ht_trendmode']   = abstract.HT_TRENDMODE(df) # no normal?

    # Pattern Recognition
    # for fn in talib.get_function_groups()['Pattern Recognition']:
    #     df[fn] = (abstract.Function(fn))(df)

    # target
    df['target']      = abstract.ROC(df, timeperiod=10).shift(-11) # no normal

    ######### 항목 생성 끝

    return df


def get_tot_df(ohlcvs
                , tot_idx
                , filename = r"D:\2022\CoinbizExpert\data_feature.pickle"
                , force_download = False):
                
    if force_download or  not os.path.exists(filename):
    
        tot_set = pd.DataFrame(index = tot_idx)

        i = 0        
        for key, val in ohlcvs.items():

            # 비어있는 index 채우기
            tot_ohlcv = pd.DataFrame(index = tot_idx).join(val, how = 'left')

            # 결측으로 시작하는 경우, 해당 코인 제외
            if tot_ohlcv.head(1).isna().iloc[0,0]: continue

            # 거래가 없어서 결측인 경우 처리
            tot_ohlcv.close.fillna(method = 'ffill', inplace = True)
            tot_ohlcv.open.fillna(tot_ohlcv.close, inplace = True)
            tot_ohlcv.high.fillna(tot_ohlcv.close, inplace = True)
            tot_ohlcv.low.fillna(tot_ohlcv.close, inplace = True)
            tot_ohlcv.value.fillna(tot_ohlcv.close, inplace = True)
            tot_ohlcv.volume.fillna(value = 0, inplace = True)
            
            # 항목 생성
            tot_ohlcv = create_features(tot_ohlcv, tp = 10, multiple = 3, repeat = 6)

            # raw column 삭제
            tot_ohlcv.drop(columns=['open','high','low','close','value'], inplace=True)
            
            # 항목명 변경
            tot_ohlcv.rename(columns = {c:key+'-'+c for c in tot_ohlcv.columns}, inplace=True)

            # tot_set에 합치기
            tot_set = tot_set.join(tot_ohlcv)

            i += 1
            if i > 30: break

        with open(filename,"wb") as fw:
            pickle.dump(tot_set, fw)   
    else:

        # Load pickle
        with open(filename,"rb") as fr:
            tot_set = pickle.load(fr)  

        