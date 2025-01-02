import csv
import bs4
import requests
import pandas as pd

headers = {
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0',
    'Connection': 'keep-alive'
}


# @task(name="获取html")
def getHtml(url):
    # 设置抓取的次数
    retry_count = 2
    # proxy = get_proxy().get("proxy")
    while retry_count > 0:
        try:
            html = requests.get(f'https://www.12365auto.com{url}', headers=headers, timeout=3)
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
    pipeline(i1=14000, i2=16000)


res_sel["典型问题"] = res_sel["典型问题"].str[:-1].str.split(',')

# convert list of pd.Series then stack it
res_ = (res_sel.set_index(["投诉内容"])["典型问题"].apply(pd.Series).stack().reset_index().drop('level_1', axis=1).rename(columns={0:"典型问题"}))
res_["labels_count"] = res_.groupby(["典型问题"]).transform("count")
res_["典型问题"].value_counts().reset_index()
res[res["投诉内容"].str.contains("2023年提车比亚迪秦plus21款DM-i55km旗舰型，当时购买价款125800元")]

labels = []
for d in data:
    v = d["value"]
    for it in d["items"]:
        id = it["id"]
        labels.append(f"{v}{id}")
