
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from  sqlalchemy.exc import IntegrityError
from datetime import datetime

Base = declarative_base()

"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"
scoreUrlFormat = "https://www.meijutt.com/inc/ajax.asp?id={}&action=newstarscorevideo"
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
}



class Link(Base):
    # 表的名字:
    __tablename__ = 'LINK'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  #  VDPP,
    title = Column(String)  #  observation,
    source = Column(String)  # observation,
    mod_date = Column(DateTime)

class Page(Base):
    # 表的名字:
    __tablename__ = 'PAGE'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  #  VDPP,
    title = Column(String)  #  observation,
    info = Column(String)  # observation,
    des = Column(String)  # observation,
    mod_date = Column(DateTime)

class Score(Base):
    # 表的名字:
    __tablename__ = 'SCORE'

    def __init__(self):
        pass

    # 表的结构:
    source = Column(String, primary_key=True)  #  VDPP,
    s1 = Column(Integer)
    s2 = Column(Integer)
    s3 = Column(Integer)
    s4 = Column(Integer)
    s5 = Column(Integer)
    total = Column(Integer)
    mean = Column(Float)
    mod_date = Column(DateTime)

def getPage(soup:BeautifulSoup,source:str):
    title = None
    try:
        title = soup.title.text
        print(title)
    except:
        pass
    metadata = None
    try:
        metadata = str(soup.select_one('div>.info-box'))
    except:
        pass
    des = None
    try:
        des = soup.select_one('div>.des').text
    except:
        pass
    page = Page()
    page.link = source
    page.mod_date = datetime.now()
    page.title = title
    page.des = des
    page.info = metadata
    return page


def getLinks(soup:BeautifulSoup,source:str):
    aTags:[Tag] = soup.findAll(name='a')  # 使用属性字典方式
    links: [Link] = []
    for aTag in aTags:
        link = Link()
        link.link = aTag.attrs.get('href')
        link.source = source
        link.title = aTag.attrs.get('title')
        link.mod_date = datetime.now()
        links.append(link)
    return links


def readPages(source:str, index=False)->dict:
    print("{}   {}".format(datetime.now().isoformat(),source))
    ret = requests.get(url=source, headers=headers, timeout=(30, 30))
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    soup:BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快
    page = getPage(soup,source)
    links = getLinks(soup,source)
    if not index and (page.info is None or len(page.info)) <= 40:
        print("Skip this page as desc too short")
        print(page.des)
        links = []
        page = None
    print("Has {} links".format(len(links)))
    return {'page':page,'links':links}

def readScore(source)->Score:
    import re
    videoId = ""
    try:
        videoId = re.findall('https://www.meijutt.com/content/meiju([0-9]+).html', source)[0]
    except:
        return None

    print("{} Get score of {}".format(datetime.now().isoformat(),videoId))
    url = scoreUrlFormat.format(videoId)
    ret = requests.get(url=url, headers=headers, timeout=(30, 30))
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    dt:[int] = ret.json()
    print("id {} Score {}".format(videoId, dt))
    score = Score()
    score.source = source
    score.s1 = dt[0]
    score.s2 = dt[1]
    score.s3 = dt[2]
    score.s4 = dt[3]
    score.s5 = dt[4]
    score.total = score.s1 + score.s2 + score.s3 + score.s4 + score.s5
    score.mean = (score.s1*2 + score.s2*4 + score.s3*6 + score.s4*8 + score.s5*10)
    if score.total and score.total>0:
        score.mean = score.mean/score.total
    else:
        score.mean = None
    score.mod_date = datetime.now()
    return score


def insertOrIgnoreAll(objs,engine):
    Session = sessionmaker(bind=engine)
    count = 0
    for link in objs:
        try:
            session = Session()
            session.add(link)
            session.commit()
            count += 1
        except IntegrityError as e:
            pass

def savePage(page:dict,engine):
    Session = sessionmaker(bind=engine)
    try:
        session = Session()
        pageDAO = page.get('page')
        session.merge(pageDAO)
        session.commit()
    except Exception as e:
        print("Error save page {} with {}".format(page,str(e)))

    try:
        links = page.get('links')
        insertOrIgnoreAll(links,engine)
    except Exception as e:
        print("Error save links {} with {}".format(page,str(e)))

def getNews(engine):
    import re
    seed = 'https://www.meijutt.com/new100.html'
    seedLinks = []
    seedLinks.extend(readPages(seed,index=True).get('links'))
    for link in seedLinks:
        source:str = link.link
        if re.match('/content/meiju[0-9]+.html',source) is None:
            print("Skip source {} for not match regexp".format(source))
            continue
        if re.match('^http.*',source) is None:
            source = "{}{}".format('https://www.meijutt.com',source)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                savePage(page,engine)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))

def getAll(engine):
    dp = 1
    i = 24327
    while i< 300000:
        # source = 'https://www.coshi.cc/Mov/{}.html'.format(i)
        i = i+1
        source = 'https://www.meijutt.com/content/meiju{}.html'.format(i)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                if dp > 1:
                    i = int(i-dp)
                dp = 1
                savePage(page,engine)
            else:
                dp = 1.1*dp
                print("dp={}".format(dp))
                i = int(i + dp)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))


def getAllScores(engine):
    import random
    import re
    Session = sessionmaker(bind=engine)

    while True:
        try:
            session = Session()
            from sqlalchemy.sql import exists
            results = session.query(Page).filter(Page.link.like('https://www.meijutt.com/content/meiju%.html'))\
                .filter(~exists().where(Page.link == Score.source)).order_by('des').limit(2000).all()
            session.commit()
            if len(results) == 0:
                break
            index = random.randrange(len(results))
            page:Page = results[index]
            link = page.link
            score = readScore(link)
            session = Session()
            session.merge(score)
            session.commit()
        except Exception as e:
            print("Error download torrent for {}".format(e))


def main():
    engine = create_engine('sqlite:///./data/meijutt.com.db')
    Base.metadata.create_all(engine)
    # getAllScores(engine)
    getNews(engine)


    return


if __name__ == '__main__':
    main()