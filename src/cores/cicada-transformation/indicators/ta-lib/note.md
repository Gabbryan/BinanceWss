-DL les fichiers parquets et les extraires dans une df (concaténer tous les fichier parquet daily de la date de début à la date de fin)

- Considérer la date de début de calcul comme la date d'acquisition des données nécéssaires à l'inisialisation de l'indicateur
  EX: RSI 14 prendre la data 14 jours avant la déclaration de la date de début
  (df modulable en fonction d'une date d'entrée et d'une date de fin de calcul d'indicateurs)

  INPUT : Date_range, timeframe, max_params_indicators:
  (max params_indicators) : fenetre maximale requise par un indicateur dans l'appels de la fonction pour le calcul
  Si RSI 14
  df0 = GCScontroller.extract_parquet_to_df(index-pd.Timedelta(day=14), timeframe )

                                def extract klines_to_df: -----> controller_transformation
                                    if timeframe > convert(1d):
                                        df0 = GCScontroller.extract_parquet_to_df(index-pd.Timedelta(day=14), timeframe )
                                    elif df0 = GCScontroller.extract_parquet_to_df(index-pd.Timedelta(weeks=14), timeframe )

        
                            
                                def extract_parquet_to_df(self, object) aggTrades, trades, klines, volumes_bars  ----> GCSController
                                            
    
    - Concaténer les DF entre eux
    - talib inputs : list: INDICATORS, dict: list 
    

    
    prendre en compte : - timeframes
    - 
    
    Dataframe Format:
    Klines (Time Bars)
    data.parquet
    OHLCV    RSI     MACD   EWMA(10) ...  SUPERTREND   

Klines ----->>
PATH : Trustia-data/Transformation/market/brokers/klines/pairs/timeframes/year/month/day
files : indicators
market : -Forex
-Crypto/exchange
-...

year/month/days

exchange : Binance
Kucoin

Volumes Bars ---->
PATH : Trustia-data/Transformation/bars/market/pairs/Volumes_bars/indicators

path_output = "Transformed/{assets}/{brokers}/{bars}/{symbol}/{timeframe}/{indicators}/data_{start_year:02d}_{start_month:02d}_{start_day:02d}_{end_year:02d}_
{end_month:02d}_{end_day:02d}"
