# -*- coding: utf-8 -*-
import os
import socket
import requests


class FeishuUtil(object):
    def __init__(self, logger=None):
        self.__host = None
        self.__commit_id = None
        self.__avg_history = None
        self.__current_value = None
        self.__tc_desc = None
        self.__scenario = None
        self.__branch = None
        self.__group_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/4f87b1ec-89b6-4f15-a510-5a427b903a61'
        self.__logger = logger

    def set_host(self, host: str):
        self.__host = host

    def set_scenario(self, scenario: str):
        self.__scenario = scenario

    def set_tc_desc(self, tc_desc: str):
        self.__tc_desc = tc_desc

    def set_current_value(self, current_value: str):
        self.__current_value = current_value

    def set_avg_history(self, avg_history: str):
        self.__avg_history = avg_history

    def set_branch(self, branch: str):
        self.__branch = branch

    def set_commit_id(self, commit_id: str):
        self.__commit_id = commit_id

    def __get_msg(self, text: str):
        return {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "Performance Testing Alert",
                        "content": [
                            [{
                                "tag": "text",
                                "text": text
                            }
                            ]]
                    }
                }
            }
        }

    def send_msg(self):
        text = f'''        desc       : {self.__host}
        scenario   : {self.__scenario}
        test_case    : {self.__tc_desc}
        current_value  : {self.__current_value}
        history_value: {self.__avg_history}
        branch     : {self.__branch}
        commit_id  : {self.__commit_id}\n'''

        json = self.__get_msg(text)
        headers = {
            'Content-Type': 'application/json'
        }

        req = requests.post(url=self.__group_url, headers=headers, json=json)
        inf = req.json()
        if "StatusCode" in inf and inf["StatusCode"] == 0:
            pass
        else:
            self.__logger.warning("执行失败，返回信息：{0}".format(inf.stdout.decode('utf-8').strip()))


def main():
    hostname = socket.gethostname()
    try:
        feishuUtil = FeishuUtil()
        feishuUtil.set_host(hostname)
        # feishuUtil.set_scenario()
        feishuUtil.send_msg()
    except Exception as e:
        print("exception:", e)
    exit(1)


if __name__ == '__main__':
    main()
