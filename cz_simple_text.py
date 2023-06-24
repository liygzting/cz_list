import csv
import bs4
import requests
import pandas as pd
# from dask.distributed import Client
# from prefect import task, flow
# from prefect_dask import DaskTaskRunner
# import threading

# client = Client(n_workers=8, threads_per_worker=1, processes=False)

headers = {
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Connection': 'keep-alive'
}


# def get_proxy():
#     return requests.get("http://127.0.0.1:5010/get/").json()
#
#
# def delete_proxy(proxy):
#     requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))
#

# your spider code
# @task(name="获取html")
def getHtml(url):
    # 设置抓取的次数
    retry_count = 2
    # proxy = get_proxy().get("proxy")
    while retry_count > 0:
        try:
            html = requests.get(f'https://www.12365auto.com{url}', headers=headers)
            # 使用代理访问
            return html
        except Exception:
            retry_count -= 1
    # # 删除代理池中代理
    # delete_proxy(proxy)
    return None


# @task(name="解析html")
def parseHtml(url, outfile):
    html = getHtml(url)
    print(html)
    if html:
        print(f"crawl {url} page....")
        html.encoding = 'gbk'
        html_text = bs4.BeautifulSoup(html.text, 'html.parser')
        tsnr = html_text.find('div', attrs={'class': 'tsnr'}).text
        tsnr = tsnr.replace("\n", "")
        with open(outfile, "a", encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([url, tsnr])


# @flow(name="Flow for gethtml pipeline", task_runner=DaskTaskRunner(address=client.scheduler.address))
def pipeline(i1, i2):
    outfile = f"./datas/samples_tsnr_{i1}-{i2}.csv"
    df = pd.read_csv("./samples_index.csv")
    # df_done = pd.read_csv("samples_tsnr.csv", header=None, names=["url", "text"])
    # urls = [fi for fi in df["链接"].to_list() if fi not in df_done["url"]]
    urls = df["链接"].to_list()[i1:i2]
    for url in urls:
        print(url)
        # parseHtml.submit(url)
        parseHtml(url, outfile)


if __name__ == '__main__':
    pipeline(i1=2000, i2=4000)

# def main(filename):
#     df = pd.read_csv(filename)
#     df_done = pd.read_csv("samples_tsnr.csv", header=None, names=["url", "text"])
#     urls = [fi for fi in df["链接"].to_list() if fi not in df_done["url"]]
#     threads = []
#     for url in urls:
#         t = threading.Thread(target=parseHtml, args=(url,))
#         threads.append(t)
#
#     for t in threads:
#         t.start()
#
#     for t in threads:
#         t.join()
#
#
# if __name__ == '__main__':
#     main(filename="samples_index.csv")
