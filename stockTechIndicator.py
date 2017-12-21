import cx_Oracle as co
from alpha_vantage.techindicators import TechIndicators
import pandas as pd
from datetime import datetime
import threading


##########################################################
# Create an Oracle Database connection and insert the
# dataframe contents via the desired query
##########################################################
def insertToOracleDatabase(df, query):
    connection = co.connect('CY/WElcome_123#@129.146.87.82:1521/pdb1.sub10231952570.newvcn.oraclevcn.com')
    cursor = connection.cursor()

    rows = [tuple(x) for x in df.values]
    cursor.executemany(query, rows)
    connection.commit()


def techIndicatorPull(symbol):
    interval = "1min"
    print("Calculating Tech Indicators ...")

    ti = TechIndicators(key='1HAYLUHGCB6E0VXC', output_format='pandas')

    print("Progress: 0% complete...")

    ad_data, meta_data = ti.get_ad(symbol=symbol, interval=interval)
    adosc_data, meta_data = ti.get_adosc(symbol=symbol, interval=interval)
    adx_data, meta_data = ti.get_adx(symbol=symbol, interval=interval)
    adxr_data, meta_data = ti.get_adxr(symbol=symbol, interval=interval)
    apo_data, meta_data = ti.get_apo(symbol=symbol, interval=interval)
    aroon_data, meta_data = ti.get_aroon(symbol=symbol, interval=interval)
    aroonosc_data, meta_data = ti.get_aroonosc(symbol=symbol, interval=interval)
    atr_data, meta_data = ti.get_atr(symbol=symbol, interval=interval)
    bop_data, meta_data = ti.get_bop(symbol=symbol, interval=interval)
    cci_data, meta_data = ti.get_cci(symbol=symbol, interval=interval)

    print("Progress: 20% complete...")

    cmo_data, meta_data = ti.get_cmo(symbol=symbol, interval=interval)
    dema_data, meta_data = ti.get_dema(symbol=symbol, interval=interval)
    dx_data, meta_data = ti.get_dx(symbol=symbol, interval=interval)
    ema_data, meta_data = ti.get_ema(symbol=symbol, interval=interval)
    ht_dcperiod_data, meta_data = ti.get_ht_dcperiod(symbol=symbol, interval=interval)
    ht_dcphase_data, meta_data = ti.get_ht_dcphase(symbol=symbol, interval=interval)
    ht_phasor_data, meta_data = ti.get_ht_phasor(symbol=symbol, interval=interval)
    ht_sine_data, meta_data = ti.get_ht_sine(symbol=symbol, interval=interval)
    ht_trendline_data, meta_data = ti.get_ht_trendline(symbol=symbol, interval=interval)
    ht_trendmode_data, meta_data = ti.get_ht_trendmode(symbol=symbol, interval=interval)

    print("Progress: 40% complete...")

    kama_data, meta_data = ti.get_kama(symbol=symbol, interval=interval)
    macd_data, meta_data = ti.get_macd(symbol=symbol, interval=interval)
    macdext_data, meta_data = ti.get_macdext(symbol=symbol, interval=interval)
    mama_data, meta_data = ti.get_mama(symbol=symbol, interval=interval)
    mfi_data, meta_data = ti.get_mfi(symbol=symbol, interval=interval)
    midpoint_data, meta_data = ti.get_midpoint(symbol=symbol, interval=interval)
    midprice_data, meta_data = ti.get_midprice(symbol=symbol, interval=interval)
    minus_di_data, meta_data = ti.get_minus_di(symbol=symbol, interval=interval)
    mom_data, meta_data = ti.get_mom(symbol=symbol, interval=interval)
    natr_data, meta_data = ti.get_natr(symbol=symbol, interval=interval)

    print("Progress: 60% complete...")

    obv_data, meta_data = ti.get_obv(symbol=symbol, interval=interval)
    plus_di_data, meta_data = ti.get_plus_di(symbol=symbol, interval=interval)
    plus_dm_data, meta_data = ti.get_plus_dm(symbol=symbol, interval=interval)
    ppo_data, meta_data = ti.get_ppo(symbol=symbol, interval=interval)
    roc_data, meta_data = ti.get_roc(symbol=symbol, interval=interval)
    rocr_data, meta_data = ti.get_rocr(symbol=symbol, interval=interval)
    rsi_data, meta_data = ti.get_rsi(symbol=symbol, interval=interval)
    sar_data, meta_data = ti.get_sar(symbol=symbol, interval=interval)
    sma_data, meta_data = ti.get_sma(symbol=symbol, interval=interval)
    stoch_data, meta_data = ti.get_stoch(symbol=symbol, interval=interval)

    print("Progress: 80% complete...")

    stochf_data, meta_data = ti.get_stochf(symbol=symbol, interval=interval)
    stochrsi_data, meta_data = ti.get_stochrsi(symbol=symbol, interval=interval)
    t3_data, meta_data = ti.get_t3(symbol=symbol, interval=interval)
    tema_data, meta_data = ti.get_tema(symbol=symbol, interval=interval)
    trange_data, meta_data = ti.get_trange(symbol=symbol, interval=interval)
    trima_data, meta_data = ti.get_trima(symbol=symbol, interval=interval)
    trix_data, meta_data = ti.get_trix(symbol=symbol, interval=interval)
    ultsoc_data, meta_data = ti.get_ultsoc(symbol=symbol, interval=interval)
    willr_data, meta_data = ti.get_willr(symbol=symbol, interval=interval)
    wma_data, meta_data = ti.get_wma(symbol=symbol, interval=interval)
    bbands_data, meta_data = ti.get_bbands(symbol=symbol, interval=interval)

    print("Progress: 90% complete...")

    result = pd.concat([ad_data, adosc_data, adx_data, adxr_data, apo_data, aroon_data, aroonosc_data, atr_data,
                        bop_data, cci_data, cmo_data, dema_data, dx_data, ema_data, ht_dcperiod_data,
                        ht_dcphase_data, ht_phasor_data, ht_sine_data, ht_trendline_data, ht_trendmode_data,
                        kama_data, macd_data, macdext_data, mama_data, mfi_data, midpoint_data, midprice_data,
                        minus_di_data,
                        mom_data, natr_data, obv_data, plus_di_data, plus_dm_data, ppo_data, roc_data, rocr_data,
                        rsi_data, sar_data, sma_data, stoch_data, stochf_data, stochrsi_data, t3_data, tema_data,
                        trange_data, trima_data, trix_data, ultsoc_data, willr_data, wma_data, bbands_data], axis=1)

    result["stockticker"] = symbol

    result = result.reindex(result.index.rename('timestamp'))
    result = result.dropna(axis=0, how='any')
    result = result.round(2)
    result = result.rename(columns={'Chaikin A/D': 'ChaikinAD',
                                    'Aroon Up': 'AroonUp',
                                    'Aroon Down': 'AroonDown',
                                    'LEAD SINE': 'LeadSine',
                                    'Real Upper Band': 'RealUpperBand',
                                    'Real Middle Band': 'RealMiddleBand',
                                    'Real Lower Band': 'RealLowerBand'})

    fd = open('results/rawTechIndicatorData.csv', 'w')
    result.to_csv(fd, index=True, encoding='utf-8')
    fd.close()

    print("Progress: 99% complete...")

    df = pd.read_csv('results/rawTechIndicatorData.csv')

    insertToOracleDatabase(df, '''insert /*+ ignore_row_on_dupkey_index(stocktechindicator, stocktechindicator_pk) */
                                    into stocktechindicator 
                                    (timestamp,ChaikinAD,ADOSC,ADX,ADXR,
                                    APO,AroonUp,AroonDown,AROONOSC,ATR,
                                    BOP,CCI,CMO,DEMA,DX,
                                    EMA,DCPERIOD,HT_DCPHASE,QUADRATURE, PHASE,
                                    LEADSINE,SINE,HT_TRENDLINE,TRENDMODE,KAMA,
                                    MACD,MACD_Hist,MACD_Signal,MACD_Signal2,MACD2,
                                    MACD_Hist2,FAMA,MAMA,MFI,MIDPOINT,
                                    MIDPRICE,MINUS_DI,MOM,NATR,OBV,
                                    PLUS_DI,PLUS_DM,PPO,ROC,ROCR,
                                    RSI,SAR,SMA,SlowK,SlowD,
                                    FastK,FastD,FastK2,FastD2,T3,
                                    TEMA,TRANGE,TRIMA,TRIX,ULTOSC,
                                    WILLR,WMA,RealLowerBand,RealUpperBand,RealMiddleBand,
                                    stockticker) 
                                    values (:1, :2, :3, :4, :5, 
                                            :6, :7, :8, :9, :10,
                                            :11, :12, :13, :14, :15, 
                                            :16, :17, :18, :19, :20, 
                                            :21, :22, :23, :24, :25, 
                                            :26, :27, :28, :29, :30, 
                                            :31, :32, :33, :34, :35, 
                                            :36, :37, :38, :39, :40, 
                                            :41, :42, :43, :44, :45, 
                                            :46, :47, :48, :49, :50, 
                                            :51, :52, :53, :54, :55, 
                                            :56, :57, :58, :59, :60, 
                                            :61, :62, :63, :64, :65, 
                                            :66)''')


##########################################################
# Begin process
##########################################################
def begin():

    print("---- New Data Pull ----")
    print("Start: "+datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # every ten hours...
    threading.Timer(36000.0, begin).start()

    techIndicatorPull("ORCL")
    techIndicatorPull("MSFT")
    techIndicatorPull("AMZN")
    techIndicatorPull("IBM")

    print("---- Waiting ----")

begin()
