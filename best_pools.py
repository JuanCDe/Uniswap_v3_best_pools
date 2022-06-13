import urllib.request
import json
from datetime import datetime
import pandas as pd
import requests
import yaml

with open("config.yml", encoding="utf-8") as conf:
    config_file = yaml.safe_load(conf)


def query_thegraph(query, variables):
    '''
    :param query:
    :param variables:
    :return:
    '''
    req = urllib.request.Request("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3")
    req.add_header('Content-Type', 'application/json; charset=utf-8')
    jsondata = {"query": query, "variables": variables}
    jsondataasbytes = json.dumps(jsondata).encode('utf-8')
    req.add_header('Content-Length', len(jsondataasbytes))
    response = urllib.request.urlopen(req, jsondataasbytes)
    resp = json.load(response)
    return resp["data"]


class PoolsByFeeTVL:
    '''
    Class to collect pools info
    Disclaimer: Is it really needed a class here??
    '''
    def __init__(self, pool_limit):
        # from https://github.com/atiselsts/uniswap-v3-liquidity-math/blob/master/subgraph-liquidity-query-example.py
        # https://thegraph.com/hosted-service/subgraph/ianlapham/uniswap-v3-subgraph
        # entities https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql
        # https://docs.uniswap.org/sdk/subgraph/subgraph-examples

        # Get position current info
        query_liq_pools = """query pools($pool_limit: Int) {
                                pools (first: $pool_limit,
                                       orderBy: volumeUSD,
                                       orderDirection: desc,
                                       where: {volumeUSD_gt: 1000000}) {
                                                                        id
                                                                        token0{symbol}
                                                                        token1{symbol}
                                                                        }
                                                }
                            """
        variables_liq = {"pool_limit": pool_limit}
        obj = query_thegraph(query_liq_pools, variables=variables_liq)
        main_pools = pd.json_normalize(obj["pools"], max_level=1)
        main_pools.set_index("id", inplace=True)
        main_pools["pair"] = main_pools["token0.symbol"] + "/" + main_pools["token1.symbol"]
        main_pools.drop(["token0.symbol", "token1.symbol"], axis=1, inplace=True)

        # poolHourDatas would be the way to go, but TheGraph's response is 0 for volumeUSD and feeUSD
        # https://github.com/Uniswap/v3-subgraph/issues/79
        # I have to use poolDayDatas that is not accurate for new pools
        # query_fee_tvl = """query pools($pool: String, $since: Int) {
        #                     poolHourDatas(first: 24,
        #                                 orderDirection: desc,
        #                                 orderBy: periodStartUnix,
        #                                 where: {
        #                                         pool: $pool,
        #                                         txCount_gt: 0
        #                                         }
        #                                 ){
        #                                 periodStartUnix
        #                                 pool { id }
        #                                 tvlUSD
        #                                 volumeUSD
        #                                 feesUSD
        #                                 liquidity
        #                                 txCount
        #                                 high
        #                                 low
        #                                 close
        #                                 }
        #                         }
        #                 """

        days_since = 5
        since = int(datetime.now().timestamp()-(60*60)*24 * days_since)
        query_fee_tvl = """query pools($pool: String, $since: Int) {
                            poolDayDatas(first: 5,
                                        orderDirection: desc,
                                        orderBy: date,
                                        where: {
                                                pool: $pool,
                                                date_gt: $since
                                                txCount_gt: 0
                                                }
                                        ){
                                        pool { id }
                                        date
                                        tvlUSD
                                        volumeUSD
                                        feesUSD
                                        liquidity
                                        txCount
                                        high
                                        low
                                        close
                                        }
                                }
                        """

        print("Getting main pools' info:")
        pool_info_list = []
        for i in range(len(main_pools)):
            pool_id = main_pools.iloc[i].name
            variables_tvl_fee = {"pool": pool_id, "since": since}
            print(i, "/", len(main_pools), end="\r")
            # time.sleep(0.5)
            obj = query_thegraph(query=query_fee_tvl, variables=variables_tvl_fee)
            if not obj['poolDayDatas']:
                continue
            pool_data = pd.json_normalize(obj["poolDayDatas"], max_level=1)
            pool_info_list.append(pool_data)
        pool_info_list_calc = pd.concat([calc_on_pool_info(df) for df in pool_info_list])
        self.pool_info_complete = main_pools.join(pool_info_list_calc)
        # self.pool_info_complete = self.pool_info_complete[self.pool_info_complete["volatility"] > 0]
        self.pool_info_complete["ranking"] = cal_ranking(self.pool_info_complete)
        self.pool_info_complete.sort_values("ranking", ascending=False, inplace=True)

    def get_pools(self, top):
        '''
        :param top:
        :return:
        '''
        return self.pool_info_complete.head(top)


def calc_on_pool_info(pool_info_complete):
    '''
    :param pool_info_complete:
    :return:
    '''
    pool_info_complete = pool_info_complete.set_index("pool.id")
    pool_info_complete = pool_info_complete.apply(pd.to_numeric, errors="coerce")
    pool_info_complete["fee_tier"] = round(pool_info_complete["feesUSD"]*100/pool_info_complete["volumeUSD"], 2)
    pool_info_complete["tvl_to_vol"] = pool_info_complete["tvlUSD"] / pool_info_complete["volumeUSD"]
    pool_info_complete["fees_to_tvl"] = round(pool_info_complete["feesUSD"]*100 / pool_info_complete["tvlUSD"], 3)
    mean_close = sum(pool_info_complete["close"]) / len(pool_info_complete["close"])
    # Volatility as standard deviation of close. Not really useful for new pools or short-term strategies
    pool_info_complete["volatility"] = (sum((x - mean_close)**2 for x in pool_info_complete["close"]) / len(pool_info_complete["close"]))**0.5
    if sum(pool_info_complete["txCount"]) > 0:
        mean_tx = sum(pool_info_complete["txCount"]) / len(pool_info_complete["txCount"])
        pool_info_complete["tx_trend"] = [(mean_tx - x) for x in pool_info_complete["txCount"]]
    else:
        pool_info_complete["tx_trend"] = 0
    mean_vol = sum(pool_info_complete["volumeUSD"]) / len(pool_info_complete["volumeUSD"])
    pool_info_complete["vol_trend"] = [(mean_vol - x) for x in pool_info_complete["volumeUSD"]]
    mean_tvl = sum(pool_info_complete["tvlUSD"]) / len(pool_info_complete["tvlUSD"])
    pool_info_complete["tvl_trend"] = [(mean_tvl - x) for x in pool_info_complete["tvlUSD"]]
    return pool_info_complete.head(1)


def cal_ranking(pool_info_complete):
    '''
    :param pool_info_complete:
    :return:
    '''
    # Revert the variables "the lower, the better"
    pool_info_complete["tvl_to_vol_inv"] = 1/pool_info_complete["tvl_to_vol"]
    pool_info_complete["volatility_inv"] = 1/pool_info_complete["volatility"]

    pool_info_complete_ranked = pool_info_complete[["tvl_to_vol_inv", "volatility_inv",
                                                    "fees_to_tvl", "tx_trend", "vol_trend",
                                                    "tvl_trend"]].rank()
    pool_info_complete_ranked["fees_to_tvl"] = pool_info_complete_ranked["fees_to_tvl"] * 50
    pool_info_complete_ranked["tvl_to_vol_inv"] = pool_info_complete_ranked["tvl_to_vol_inv"] * 2
    pool_info_complete_ranked_summed = pool_info_complete_ranked.sum(axis=1)
    return pool_info_complete_ranked_summed


def create_tg_msg(poo):
    '''
    :param poo:
    :return:
    '''
    poo_sorted = poo.sort_values("fees_to_tvl", ascending=False)
    poo_sorted["volatility"] = poo_sorted["volatility"].apply(lambda x: '%.1e' % x)
    poo_tg = pd.DataFrame()
    poo_tg["Pair"] = "[" + poo_sorted["pair"] + "]" + "(https://info.uniswap.org/#/pools/" + poo_sorted.index.astype(str) + ")"
    poo_tg["Volatility"] = "[" + poo_sorted["volatility"] + "]" + "(https://dexscreener.com/ethereum/" + poo_sorted.index.astype(str) + ")"
    poo_tg["Fees2TVL"] = "[" + poo_sorted["fees_to_tvl"].astype(str) + "]" + "(https://info.uniswap.org/#/pools/" + poo_sorted.index.astype(str) + ")"
    # poo_tg["Fees2TVL"] = poo_sorted["fees_to_tvl"].astype(str)
    poo_tg["Fee"] = poo_sorted["fee_tier"]
    poo_tg["Tx"] = poo_sorted["txCount"].astype(str)
    # poo_tg.set_index("Pair", inplace=True)
    msg = "Pair\t\t|\t\tVolatility\t\t|\t\tTx\t\t|\t\tFees2TVL"
    for i in range(len(poo_tg)):
        pool_msg = f' {poo_tg["Pair"][i]}|' \
                   f' \U0001F4C9 {poo_tg["Volatility"][i]} |' \
                   f' \U0001F9EE {poo_tg["Tx"][i]} |' \
                   f' \U0001F4B8 {poo_tg["Fees2TVL"][i]}'
        msg = f'{msg}\n{pool_msg}'
    return msg


def send_tg_msg(msg, conf_file):
    '''
    :param msg:
    :param conf_file:
    :return:
    '''
    bot_token = conf_file["bot_token"]
    chat_id = conf_file["chatID"]
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    params = {'chat_id': chat_id, 'parse_mode': "Markdown", 'disable_web_page_preview': True,
              "text": msg}
    response = requests.post(url, params=params)
    if response.status_code != 200:
        params["text"] = response.text
        params["parse_mode"] = "HTML"
        requests.post(url, params=params)


best_pools = PoolsByFeeTVL(pool_limit=config_file["top_pools"])
top_pools = best_pools.get_pools(10)[["pair", "txCount", "volatility", "fee_tier", "fees_to_tvl", "ranking"]]
msg_to_send = create_tg_msg(top_pools)
send_tg_msg(msg_to_send, config_file)


# ToDo
#  - Create a better message for Telegram instead a ugly table
#  - Move from querying last x days to last x hours. Issue #79 (https://github.com/Uniswap/v3-subgraph/issues/79)
#  - Modify the script to a more pythonic style
