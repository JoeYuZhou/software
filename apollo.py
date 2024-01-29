# get token
import pickle
import shutil
import subprocess
import threading
import traceback

from apolloclient.apollo_history import ApolloHistory

import pandas as pd
import requests
import json
import datetime
import time
import xmltodict
import random
import zipfile
from shared.multithread_util import ThreadPool
from colorama import Fore

import os
from shared.util import ensure_dir, get_dict_signiture
from shared import util

from apolloclient.proposal_reader import ProposalReader

from apolloclient import PROJECT_CACHE_ROOT
from shared.di import inject
from shared.config import Config
import copy


@inject(cfg=Config)
class SSO:
    def __init__(self, cfg):
        # json replace ini
        # shorten to cfg
        # configuration["setting/apollo_path"] if not possible use get()
        self.apollo_path = cfg["setting/apollo_path"]

    def get_sso_token(self, env):
        cmd = f"runsso_{env}.bat"
        cmd_line = " ".join([cmd])
        cache_folder = PROJECT_CACHE_ROOT
        util.ensure_dir(cache_folder)
        token_file = os.path.join(cache_folder, f"token_{env}.txt")
        with open(token_file, "w+") as output:
            subprocess.call(
                cmd_line, shell=True, cwd=self.apollo_path, stdout=output, stderr=output
            )
        with open(token_file, "r") as token_f:
            token = token_f.read().rstrip()
        return token


def get_username_from_sso_token(token):
    return token.split("|")[-5]


def get_requestIds(username):
    # date_string = '01/Nov 09:47:28'
    date_string = datetime.datetime.now().strftime("%d/%b %H:%M:%S")
    # 1635740248379
    id = round(time.time() * 1000)
    # add random 5 digits for async calls
    id = str(id) + "{0:03}".format(random.randint(1, 100000))

    clientRequestId = f"{username} {date_string}"
    requestId = f"{clientRequestId} @ {id}"
    return requestId, clientRequestId


def get_body():
    file_path = util.current_dir_file(__file__, "data/request_sample.json")
    with open(file_path, "r", errors="ignore") as rf:
        body_json = json.load(rf)
    return body_json


def get_xml_body(file_path="data/trade_xml_sample.xml"):
    file_path = util.current_dir_file(__file__, file_path)
    with open(file_path, "r") as r:
        xml_string = r.read()
    json_data = xmltodict.parse(xml_string)
    return json_data


def parse_xml_to_json(xml_string):
    json_data = xmltodict.parse(xml_string)


def json_to_xml_raw(json_data):
    xml_string = xmltodict.unparse(json_data)
    return xml_string


def json_to_xml(json_data):
    xml_string = json_to_xml_raw(json_data)
    xml_string = xml_string.replace(
        '<?xml version="1.0" encoding="utf-8"?>\n', "<![CDATA["
    )
    xml_string = xml_string + "]]>"
    return xml_string


def download_file_from_url(file_url, internal_legal, ciscode, target_folder):
    util.ensure_dir(target_folder)
    file_name = os.path.basename(file_url)
    file_path = f"{target_folder}/{internal_legal}_{ciscode}_{file_name}"
    r = requests.get(file_url, allow_redirects=True)
    with open(file_path, "wb") as f:
        f.write(r.content)
    return file_path


def download_file_from_response(
    data: dict, internal_legal, ciscode, target_folder="Impact"
):
    if data["whatIfRequestStatus"] == "SUCCESS":
        file_url = data["capRequestResultDetails"][0]["diagnosticURL"]
        return download_file_from_url(file_url, internal_legal, ciscode, target_folder)


def get_proposal(proposal_path, lei_cis_map_path=None):
    """
    capitolis proposal has no ciscode and LEI while tribalance has not ciscode.
    mapping in LEI for capitolis, then mapping in ciscode for both
    TODO: add vmid into internal model, check if agreement_id is need for request. if yes, thinking about add to internal model

    vmid is later used to build request. therefore need add vmid of FOREX for the corresponding counterparty and principal

    """

    return ProposalReader()(proposal_path, lei_cis_map_path=lei_cis_map_path)


def newJsonTrade(
    tradeID,
    startDate,
    maturityDate,
    buyCurrency,
    buyNotional,
    sellCurrency,
    sellNotional,
    agreementId,
    vmId,
):
    base_xml = "data/trade_xml_sample.xml"
    xml_body_json = get_xml_body(base_xml)
    xml_body_json["FxForwardTrade"]["StartDate"] = datetime.datetime.strftime(
        startDate, "%Y-%m-%d"
    )  # .strftime('%Y-%m-%d')
    xml_body_json["FxForwardTrade"]["MaturityDate"] = datetime.datetime.strftime(
        maturityDate, "%Y-%m-%d"
    )  # .strftime('%Y-%m-%d')

    xml_body_json["FxForwardTrade"]["BuyCurrency"] = buyCurrency
    xml_body_json["FxForwardTrade"]["BuyNotional"] = round(abs(buyNotional))
    xml_body_json["FxForwardTrade"]["SellCurrency"] = sellCurrency
    xml_body_json["FxForwardTrade"]["SellNotional"] = round(abs(sellNotional))

    xml_body_json["FxForwardTrade"]["NettingSet"]["agreementId"] = agreementId
    xml_body_json["FxForwardTrade"]["NettingSet"]["collateralAgreementIds"][
        "collateralAgreementId"
    ] = agreementId
    xml_body_json["FxForwardTrade"]["ClearingHouse"]["nettingAgreementId"] = agreementId

    xml_body_json["FxForwardTrade"]["NettingSet"]["vmBucketId"] = vmId

    if vmId == "Uncollateralised":
        xml_body_json["FxForwardTrade"]["VMBucketId"] = f"{vmId}"  # 'CALYPSO-4049396'
    else:
        xml_body_json["FxForwardTrade"][
            "VMBucketId"
        ] = f"CALYPSO-{vmId}"  # 'CALYPSO-4049396'

    xml_str = json_to_xml(xml_body_json)
    # trade_id = f"Trade{index + 1}" if pd.isnull(row['trade_id']) else row['trade_id']
    trade_id = tradeID
    trade = {"tradeType": "FxForwardTrade", "tradeXml": xml_str, "id": trade_id}

    return trade


from datetime import timedelta


def proposal_to_trades(
    df_proposal: pd.DataFrame,
    base_xml="data/trade_xml_sample.xml",
    switch_pay_rec=False,
):
    trades = []
    df_proposal.reset_index(inplace=True)
    for index, row in df_proposal.iterrows():
        startDate = row["trade_date"]
        maturityDate = row["maturity_date"]
        buyCurrency = row["receive_currency"]
        buyNotional = row["receive_notional"]
        sellCurrency = row["pay_currency"]
        sellNotional = row["pay_notional"]
        agreementId = row["master_agreement"]
        vmId = row["vm_credit_support"]

        swapDays = timedelta(days=365)

        maturityDateDT = maturityDate
        startDateDT = startDate
        # maturityDateDT = datetime.datetime.strptime(maturityDate,'%d/%m/%Y')
        # startDateDT = datetime.datetime.strptime(startDate,'%d/%m/%Y')

        tradeID = f"Trade{index + 1}" if pd.isnull(row["trade"]) else row["trade"]

        trades.append(
            newJsonTrade(
                tradeID,
                startDateDT,
                maturityDateDT,
                buyCurrency,
                buyNotional,
                sellCurrency,
                sellNotional,
                agreementId,
                vmId,
            )
        )

    return trades


def build_whatif_request_body(
    username,
    businessDate,
    ciscode,
    externalLegalEntityName,
    internal_legal,
    proposal_trades,
    simCount,
):
    body = get_body()
    body["user"] = username
    body["requestContext"]["userId"] = (
        username.split("_")[1] if len(username.split("_")) > 1 else username
    )
    body["requestContext"]["businessDate"] = businessDate
    body["requestContext"]["eleDetails"]["externalLegalEntity"]["identifier"] = ciscode
    body["requestContext"]["eleDetails"][
        "externalLegalEntityName"
    ] = externalLegalEntityName
    body["requestContext"]["internalLegalEntity"]["identifier"] = internal_legal
    body["cluWhatIfScenarioData"]["newTrades"]["newTrades"] = proposal_trades
    body["cluWhatIfScenarioData"]["whatIfScenarios"]["whatIfScenarios"][0]["newTrades"][
        "newTradeReves"
    ] = []
    for trade in body["cluWhatIfScenarioData"]["newTrades"]["newTrades"]:
        body["cluWhatIfScenarioData"]["whatIfScenarios"]["whatIfScenarios"][0][
            "newTrades"
        ]["newTradeReves"].append({"idref": trade})

    body["calculationContext"]["simCount"] = simCount
    return body


def send_request_new(
    history,
    username=None,
    token=None,
    ciscode="BMLILGB",
    internal_legal="BRBOSGB",
    externalLegalEntityName="MERRILL LYNCH INTERNATIONAL",
    env="uat",
    proposal_trades=None,
    businessDate="2022-02-01",
    target_folder="Impact",
    forced=False,
    sim_count=1,
):
    if env == "prod":
        url = "http://phoenixprod.apollo.fm.rbsgrp.net:1199/WhatIfRequestWebServer/whatif/submit/"
    else:
        url = "http://LONRS07761:1199/WhatIfRequestWebServer/whatif/submit/"

    xml_body_json = build_theta_request_body(
        username,
        businessDate,
        ciscode,
        externalLegalEntityName,
        internal_legal,
        sim_count=sim_count,
    )
    manual_result = manual_result_hack(ciscode, internal_legal, target_folder)
    if not forced and manual_result is not None:
        return manual_result
    # check cache
    signature_body = copy.deepcopy(xml_body_json)
    signature_body["url"] = url

    cached_response = get_cached_response(signature_body)
    if not forced and cached_response is not None:
        print(
            f"{ciscode} cache response found. target file: {cached_response['capRequestResultDetails'][1]['diagnosticURL']}"
        )
        return cached_response

    """
    new added logic:
    > if not forced, and the signature_body's signiture is found in csv, and the status is "processing", then try to 
        download the file with get_request_result with same reqeustId
        >>if the file can be download and succeeded to be unzipped, then set the status as "success"
        >>if the file can not be download or any error, then set the status as "failed"
    > record the requestId and the signiture of signature_body in a csv file
    > set status as "processing" in the csv file
   
    > if the everything successed, update status to "success"
    > else log the error in the history as update status as failed

    """
    # if not forced, and the signature_body is found in csv, and the status is "processing", then try to
    #   download the file with get_request_result with same reqeustId
    request_signiture = get_dict_signiture(signature_body)

    if not forced:
        df = history.df
        previous_uncomplete_request = df[
            (df["signature"] == request_signiture) & (df["status"] == "processing")
        ]
        if not previous_uncomplete_request.empty:
            requestId = previous_uncomplete_request.iloc[0]["requestId"]

            data = process_response(
                ciscode,
                env,
                internal_legal,
                requestId,
                signature_body,
                target_folder,
                token,
                username,
                history,
                continue_on_error=True,
            )
            if data is not None:
                return data

    requestId, clientRequestId = get_requestIds(username)
    xml_body_json["CluWhatIfRequest"]["@requestId"] = requestId
    xml_body_json["CluWhatIfRequest"]["@clientRequestId"] = clientRequestId

    # record the requestId and the signiture of signature_body in a csv file
    history.add(
        {
            "requestId": requestId,
            "signature": request_signiture,
            "request_param": json.dumps(signature_body),
            "cis": ciscode,
            "status": "processing",
        }
    )
    history.update()
    xml_body_json["CluWhatIfRequest"]["ssoToken"] = token
    xml_str = json_to_xml_raw(xml_body_json)
    headers = {
        "Content-Type": "application/xml",
        "Accept": "application/xml, text/xml, application/*+xml",
    }
    req = requests.Session()
    try:
        req.post(url=url, data=xml_str, headers=headers, verify=False)
    except Exception as e:
        # catch and record all error but proceed forward. This is because Apollo may abort the connection
        # for whatever reason (timeout, server restart,ect)
        print(f"{requestId} error: {str(e)}")
        history.update_error(requestId, f"{str(e)}|{traceback.format_exc()}")
        history.update()
    data = process_response(
        ciscode,
        env,
        internal_legal,
        requestId,
        signature_body,
        target_folder,
        token,
        username,
        history,
        continue_on_error=False,
    )
    return data


def process_response(
    ciscode,
    env,
    internal_legal,
    requestId,
    signature_body,
    target_folder,
    token,
    username,
    history,
    continue_on_error,
):
    try:
        # below will try to get the result base on requestId,
        # if request is "PENDING" it will try for 5 hours until a success or failure returned
        # otherwise it raised a timeout error
        signature = get_dict_signiture(signature_body)
        data = get_request_result(
            username, token, requestId, signature=signature, env=env
        )
        # data = r.json()
        response_handler(ciscode, data, internal_legal, target_folder)
        # will not cache error response if above response_handler found any error because it throws exception
        cache_response(signature_body, data)
        # if everything successed, update status to "success"
        history.update_status(requestId, "success")
        history.update()
        return data
    except Exception as e:
        # any problem then update status as failed
        history.update_status(requestId, "failed")
        history.update_error(requestId, f"{str(e)}|{traceback.format_exc()}")
        history.update()
        if continue_on_error:
            return None
        else:
            raise


# def send_request_new(username=None, token=None, ciscode='BMLILGB', internal_legal='BRBOSGB',
#                          externalLegalEntityName='MERRILL LYNCH INTERNATIONAL', env='uat',
#                          proposal_trades=None, businessDate='2022-02-01', target_folder='Impact', forced=False,
#                          simCount=1000):
#     print(f'@send_request_new - begin')
#
#     if env == 'prod':
#         url = 'http://phoenixprod.apollo.fm.rbsgrp.net:1199/WhatIfRequestWebServer/whatif/submit/'
#     else:
#         url = 'http://LONRS07761:1199/WhatIfRequestWebServer/whatif/submit/'
#     body = build_whatif_request_body(username, businessDate, ciscode, externalLegalEntityName, internal_legal,
#                                      proposal_trades, simCount)
#
#     # check cache
#     signature_body = body.copy()
#     signature_body['url'] = url
#     cached_response = get_cached_response(signature_body)
#     if not forced and cached_response is not None:
#         print(
#             f"{ciscode} cache response found. target file: {cached_response['capRequestResultDetails'][1]['diagnosticURL']}")
#         return cached_response
#
#     requestId, clientRequestId = get_requestIds(username)
#     headers = {'user': username, 'ssoToken': token, 'requestId': requestId, 'clientRequestId': clientRequestId}
#     body_final = body.copy()
#     body_final['requestId'] = requestId
#     body_final['clientRequestId'] = clientRequestId
#     body_final['ssoToken'] = token
#     req = requests.Session()
#     print(f'@send_request_new - before post url:{url}')
#     result = req.post(url=url, json=body_final, headers=headers, verify=False)
#     print(f'@send_request_new - result {result}')
#     data = get_request_result(username, token, requestId, env=env)
#     # data = r.json()
#     response_handler(ciscode, data, internal_legal, target_folder)
#     cache_response(signature_body, data)
#     return data


def post_response(target_folder, principal, counterparty, result_zip_path):
    process_folder = os.path.join(target_folder, "_processed")
    util.ensure_dir(process_folder)
    zip_name = os.path.basename(result_zip_path)
    new_result_zip_path = os.path.join(
        process_folder, f"{principal}_{counterparty}_{zip_name}"
    )
    shutil.move(result_zip_path, new_result_zip_path)


def response_handler(ciscode, data, internal_legal, target_folder):
    if "whatIfRequestStatus" in data and data["whatIfRequestStatus"] == "ERROR":
        msg = f'{ciscode}:{data["error"]}'
        print(Fore.RED + f'{ciscode}:{data["error"]}')
        raise Exception(msg)
    elif "errorMessage" in data and len(data["errorMessage"]) > 0:
        print(Fore.RED + f'{ciscode}:{data["errorMessage"]}')
        raise Exception(f'{ciscode}:{data["errorMessage"]}')
    else:
        file_path = download_file_from_response(
            data, internal_legal, ciscode, target_folder=target_folder
        )
        target_path = f"{target_folder}/{internal_legal}_{ciscode}"
        unzip_file(file_path, target_path)
        post_response(target_folder, internal_legal, ciscode, file_path)


def get_request_result(
    username, token, requestId, signature, sleep=60, cut_off=18000, env="uat"
):
    """
    cut_off in seconds
    """
    if env == "prod":
        url = "http://phoenixprod.apollo.fm.rbsgrp.net:1199/WhatIfRequestWebServer/whatif/lookup/"
    else:
        url = "http://LONRS07761:1199/WhatIfRequestWebServer/whatif/lookup/"
    file_path = util.current_dir_file(__file__, r"data\result_query_sample.xml")
    xml_body_json = get_xml_body(file_path)
    xml_body_json["GetCluWhatIfRequest"]["user"] = username
    xml_body_json["GetCluWhatIfRequest"]["ssoToken"] = token
    xml_body_json["GetCluWhatIfRequest"]["requestId"] = requestId
    xml_str = json_to_xml_raw(xml_body_json)
    headers = {
        "Content-Type": "application/xml",
        "Accept": "application/xml, text/xml, application/*+xml",
    }
    req = requests.Session()
    r = req.post(url=url, data=xml_str, headers=headers, verify=False)

    # cache the response for potential troubleshooting
    cache_folder = os.path.join(PROJECT_CACHE_ROOT, "request", signature)
    util.ensure_dir(cache_folder)
    cache_path = os.path.join(
        cache_folder, datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".pkl"
    )
    with open(cache_path, "wb") as f:
        pickle.dump(r, f)

    result = r.content
    json_response = xmltodict.parse(result)
    data = json_response["CluWhatIfRequestAndResult"]["WhatIfRequestResult"]
    while data["whatIfRequestStatus"] in ["PENDING"]:
        if cut_off > sleep:
            # request not yet complete, wait
            time.sleep(sleep)
            cut_off -= sleep
            return get_request_result(
                username, token, requestId, signature, sleep, cut_off, env
            )
        else:
            raise Exception(f"request look up for {requestId} timed out")

    return data


def get_cache_file_path(body):
    signiture = get_dict_signiture(body)
    cached_folder_path = os.path.join(PROJECT_CACHE_ROOT, "succeed")
    util.ensure_dir(cached_folder_path)
    cached_file_path = os.path.join(cached_folder_path, f"request_{signiture}.pkl")
    return cached_file_path


def cache_response(body, response):
    cached_file_path = get_cache_file_path(body)
    with open(cached_file_path, "wb") as f:
        pickle.dump(response, f, protocol=pickle.HIGHEST_PROTOCOL)
    return cached_file_path


def get_cached_response(body):
    cached_file_path = get_cache_file_path(body)
    return read_pickle(cached_file_path)


def read_pickle(cached_file_path):
    if os.path.exists(cached_file_path):
        with open(cached_file_path, "rb") as f:
            respone = pickle.load(f)
        return respone
    else:
        return None


def unzip_file(file_path, target_path):
    with zipfile.ZipFile(file_path) as z:
        z.extractall(target_path)


def apollo_result_handler(results):
    report = pd.DataFrame()
    for ciscode, result in results:
        result_dict = {
            "ciscode": ciscode,
            "request_id": result["@requestId"],
            "status": result["whatIfRequestStatus"],
            "start_time": result["startTime"],
            "end_time": result["endTime"],
        }
        report = report.append(result_dict, ignore_index=True)
    if not report.empty:
        report["elapsed_time"] = report.end_time.astype(
            "datetime64[ns]"
        ) - report.start_time.astype("datetime64[ns]")
    return report


def proposal_processor_mp(
    proposal_path="proposal.xlsx",
    businessDate="2021-10-29",
    token=None,
    target_folder="Impact",
    switch_pay_rec=False,
    username=None,
    threads=1,
    env="uat",
    forced=False,
    simCount=1000,
):
    history = ApolloHistory(PROJECT_CACHE_ROOT, lock=threading.Lock())
    ciscodes, get_by_ciscode, results = proposal_processor(
        businessDate,
        proposal_path,
        token,
        target_folder,
        switch_pay_rec,
        username=username,
        history=history,
        env=env,
        forced=forced,
        simCount=simCount,
    )
    pool = ThreadPool(threads)
    pool.map(get_by_ciscode, ciscodes)
    print(ciscodes)
    # print(results)
    pool.wait_completion()
    print("complete")
    report_df = apollo_result_handler(results)
    report_df.to_csv(
        os.path.join(target_folder, "apollo_request_report.csv"), index=False
    )
    return results


def proposal_processor_sp(
    proposal_path="proposal.xlsx",
    businessDate="2021-10-29",
    token=None,
    target_folder="Impact",
    switch_pay_rec=False,
    username=None,
    threads=1,
    env="uat",
    forced=False,
    simCount=1000,
):
    history = ApolloHistory(PROJECT_CACHE_ROOT, lock=threading.Lock())
    ciscodes, get_by_ciscode, results = proposal_processor(
        businessDate,
        proposal_path,
        token,
        target_folder,
        switch_pay_rec,
        username=username,
        history=history,
        env=env,
        forced=forced,
        simCount=simCount,
    )

    for ciscode in ciscodes:
        get_by_ciscode(ciscode)

    print("complete")
    return results


def proposal_processor(
    businessDate,
    proposal_path,
    token,
    target_folder,
    switch_pay_rec,
    username,
    history,
    env="uat",
    forced=False,
    simCount=1000,
):
    if token is None:
        token = SSO().get_sso_token(env)
    if username is None:
        username = get_username_from_sso_token(token)
    ensure_dir(target_folder)
    df_proposal = get_proposal(proposal_path)
    results = []
    ciscodes = list(df_proposal["counterparty"].unique())

    def get_by_ciscode(ciscode):
        df_proposal_cis = df_proposal[df_proposal.counterparty == ciscode].copy()
        counterparty_name = "NA"
        cis_proposal_trades = proposal_to_trades(
            df_proposal_cis,
            base_xml="data/trade_xml_sample.xml",
            switch_pay_rec=switch_pay_rec,
        )  # trade_xml_sample_min.xml or trade_xml_sample.xml

        # print(cis_proposal_trades)

        response = send_request_new(
            history=history,
            username=username,
            token=token,
            ciscode=ciscode,
            externalLegalEntityName=counterparty_name,
            proposal_trades=cis_proposal_trades,
            businessDate=businessDate,
            target_folder=target_folder,
            env=env,
            forced=forced,
            sim_count=simCount,
        )
        results.append((ciscode, response))

    return ciscodes, get_by_ciscode, results


def build_theta_request_body(
    username,
    businessDate,
    ciscode,
    externalLegalEntityName,
    internal_legal,
    sim_count=1,
):
    file_path = util.current_dir_file(__file__, r"data\request_sample.xml")
    xml_body_json = get_xml_body(file_path)
    xml_body_json["CluWhatIfRequest"]["RequestContext"]["BusinessDate"] = businessDate
    xml_body_json["CluWhatIfRequest"]["RequestContext"]["internalLegalEntity"][
        "Identifier"
    ] = internal_legal
    xml_body_json["CluWhatIfRequest"]["RequestContext"]["eleDetails"][
        "externalLegalEntity"
    ]["Identifier"] = ciscode
    # xml_body_json['CluWhatIfRequest']['RequestContext']['eleDetails'][
    #     'externalLegalEntityName'] = externalLegalEntityName
    xml_body_json["CluWhatIfRequest"]["calculationContext"]["simCount"] = sim_count

    xml_body_json["CluWhatIfRequest"]["RequestContext"]["UserId"] = xml_body_json[
        "CluWhatIfRequest"
    ]["user"] = username

    return xml_body_json


def manual_result_hack(ciscode, internal_legal, target_folder):
    expected_result_folder_path = os.path.join(
        target_folder, f"{internal_legal}_{ciscode}"
    )
    if os.path.exists(expected_result_folder_path):
        msg = "unzipped result folder found"
        print(msg)
        return msg

    return None
