import GetOldTweets3 as got
from datetime import datetime, timedelta
import sys
import os
import time
from multiprocessing import Pool


output_path = '/home/metehan/social_media/data/'
if not os.path.exists(output_path):
    os.makedirs(output_path)

"""
RATE LIMITE TAKILIYORUM
Rotation proxy kullanmak lazım
"""

PROXY = "1.1.1.1:8080"

BIST100 = [
    "YYLGD", "ALKIM", "KOZAL", "TTKOM", "VESTL", "TRGYO", "TCELL", "PETKM", "SISE", "CEMTS", "GOZDE", "MGROS", "ENKAI",
    "ISMEN", "NUGYO", "TSKB", "HALKB", "AKSEN", "YKBNK", "TMSN", "ALGYO", "DOAS", "GENIL", "VAKBN", "DOHOL", "SKBNK",
    "AKBNK", "ISGYO", "KARSN", "GARAN", "TTRAK", "KOZAA", "BIMAS", "TKFEN", "FROTO", "KARTN", "TOASO", "TUPRS", "KORDS",
    "DEVA", "ODAS", "LOGO", "ERBOS", "GUBRF", "KRDMD", "GLYHO", "ALBRK", "TURSG", "ECILC", "GSDHO", "PRKAB", "YATAS",
    "TSPOR", "ASELS", "CIMSA", "SNGYO", "KCHOL", "AGHOL", "AEFES", "TUKAS", "TAVHL", "SASA", "PGSUS", "IPEKE", "ULKER",
    "EGEEN", "CCOLA", "BRYAT", "AKFGY", "BAGFS", "OTKAR", "EREGL", "SELEC", "ARCLK", "ISCTR", "BUCIM", "EKGYO", "SAHOL",
    "JANTS", "THYAO", "ALARK", "HEKTS", "VESBE", "AKSA", "BERA", "NTHOL", "ISFIN", "QUAGR", "ENJSA", "AYDEM", "OYAKC",
    "MAVI", "KONTR", "SMRTG", "PSGYO", "ISDMR", "GESAN", "GWIND", "SOKM", "BASGZ"
]
PORTFOLIO = BIST100
start = datetime.strptime("2019-01-01", "%Y-%m-%d")
end = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
date_generated = [start + timedelta(days=x) for x in range(0, (end - start).days, 5)]   # Date chunks
print("Downloading tweets for ", len(PORTFOLIO), "number of stocks start_date:", start, "end_date:", end)


def download_tweet_for_stock(stock):
        print(stock, "downloading tweets")
        outputFile = open(output_path + stock + ".csv", "w", encoding="utf8")
        # "-" + start_date + "-" + end_date + "-" + ".csv", "w+", encoding="utf8")
        outputFile.write('date,username,to,replies,retweets,favorites,text,geo,mentions,hashtags,id,permalink\n')
        for start_date in date_generated:
            try:
                end_date = (start_date + timedelta(days=5)).strftime("%Y-%m-%d")
                start_date = start_date.strftime("%Y-%m-%d")
                # print(start_date, end_date)
                # if stock == "MAVI":
                #     searh_query = "#"+stock
                # else:
                #     searh_query = stock
                tweetCriteria = got.manager.TweetCriteria().setQuerySearch('#'+stock).setSince(start_date).setUntil(end_date)  # .setMaxTweets(1)

                tweets = []
                for i in range(15):
                    try:
                        tweets = got.manager.TweetManager.getTweets(tweetCriteria, proxy=PROXY)
                        print(stock, len(tweets), "tweets downloaded", "from:", start_date, "end:", end_date, "try: ", i)
                        break
                    except:
                        time.sleep(1)
                        continue

                for t in tweets:
                    data = [t.date.strftime("%Y-%m-%d %H:%M:%S"),
                            t.username,
                            t.to or '',
                            t.replies,
                            t.retweets,
                            t.favorites,
                            '"' + t.text.replace('"', '""') + '"',
                            t.geo,
                            t.mentions,
                            t.hashtags,
                            t.id,
                            t.permalink]
                    data[:] = [i if isinstance(i, str) else str(i) for i in data]
                    outputFile.write(','.join(data) + '\n')

                outputFile.flush()
            except:
                # logger.exception("exception for stock: " + str(stock) + " from: " + start_date + " end: " + end_date)
                # logger.info("sleeping 30 seconds")
                # time.sleep(1)
                continue
        print(stock, "dumped to file")


pcount = 5
pool = Pool(pcount)
pool.map(download_tweet_for_stock, PORTFOLIO)

# download_tweet_for_stock("GARAN")
