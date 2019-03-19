
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import sessionmaker
from  sqlalchemy.exc import IntegrityError
from datetime import datetime
import re

Base = declarative_base()

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
}

def getMoreTorrents(content):
    if not content:
        return []
    # matches = re.findall('(http://adh.hjzlg.com/img(.+?)\.torrent)',content)
    matches = re.findall('(https?://(.+?)\.torrent)',content)
    torrents = []
    for match in matches:
        try:
            torrents.append(matches[0])
        except:
            pass
    return torrents


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

class Torrent(Base):
    # 表的名字:
    __tablename__ = 'TORRENT'

    def __init__(self):
        pass

    # 表的结构:
    hash = Column(String)  #  VDPP,
    content = Column(String)  #  observation,
    source = Column(String, primary_key=True)  # observation,
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


def getTorrent(source:str):
    import base64
    from hashlib import sha256
    print("Get torrent from {}".format(source))
    req = requests.get(source,headers=headers)
    torrent = Torrent()
    torrent.content = str(base64.b64encode(req.content),'utf-8')
    torrent.hash = sha256(req.content).hexdigest()
    torrent.source = source
    torrent.mod_date = datetime.now()
    return torrent



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
        des = soup.select_one('div>#size').text
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
        if link.title is None:
            link.title = aTag.text
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
    moreLinks = getMoreTorrents(page.des)
    for torrent in moreLinks:
        link = Link()
        link.source = source
        link.link = torrent
        link.mod_date = datetime.now()
        links.append(link)
    if not index and (page.des is None or len(page.des)) <= 40:
        print("Skip this page as desc too short")
        print(page.des)
        links = []
        page = None
    print("Has {} links".format(len(links)))
    return {'page':page,'links':links}


def insertOrIgnoreAll(objs,engine):
    Session = sessionmaker(bind=engine)
    count = 0
    for link in objs:
        try:
            session = Session()
            session.add(link)
            session.commit()
            count += 1
        # except IntegrityError as e:
        except Exception as e:
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
    seed = 'http://www.hjzlg.com/web3/YCMS_News.asp'
    seedLinks = []
    seedLinks.extend(readPages(seed,index=True).get('links'))
    for link in seedLinks:
        source:str = link.link
        if re.match('.*\.asp\?id=[0-9]+',source) is None:
            print("Skip source {} for not match regexp".format(source))
            continue
        if re.match('^http.*',source) is None:
            source = "{}/{}".format('http://www.hjzlg.com/web3',source)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                savePage(page,engine)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))



    seed = 'http://www.hjzlg.com/web5/YCMS_Art.asp'
    seedLinks = readPages(seed,index=True).get('links')
    for link in seedLinks:
        source:str = link.link
        if re.match('.*\.asp\?id=[0-9]+',source) is None:
            print("Skip source {} for not match regexp".format(source))
            continue
        if re.match('^http.*',source) is None:
            source = "{}/{}".format('http://www.hjzlg.com/web5',source)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                savePage(page,engine)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))

def getAll(engine):
    for i in range(0,400):
        source = 'http://www.hjzlg.com/web5/YCMS_ShowArt.asp?id={}'.format(i)
        # source = 'http://www.hjzlg.com/web3/YCMS_ShowNews.asp?id={}'.format(i)
        # source = 'https://www.meijutt.com/content/meiju{}.html'.format(i)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                savePage(page,engine)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))

    for i in range(0,2000):
        # source = 'http://www.hjzlg.com/web5/YCMS_ShowArt.asp?id={}'.format(i)
        source = 'http://www.hjzlg.com/web3/YCMS_ShowNews.asp?id={}'.format(i)
        # source = 'https://www.meijutt.com/content/meiju{}.html'.format(i)
        try:
            page = readPages(source)
            if len(page.get('links')) > 20:
                savePage(page,engine)
        except Exception as e:
            print("Get page {} failed {}".format(source,str(e)))

def downloadTorrents(engine):
    import random
    Session = sessionmaker(bind=engine)

    while True:
        try:
            session = Session()
            from sqlalchemy.sql import exists
            results = session.query(Link).filter(Link.link.like('%.torrent'))\
                .filter(~exists().where(Link.link == Torrent.source)).limit(50).all()
            session.commit()
            if len(results) == 0:
                break
            index = random.randrange(len(results))
            link:Link = results[index]
            torrent = getTorrent(link.link)
            session = Session()
            session.add(torrent)
            session.commit()
        except Exception as e:
            print("Error download torrent for {}".format(e))


def main():
    engine = create_engine('sqlite:///./data/hjzlg.com.db')
    Base.metadata.create_all(engine)
    getNews(engine)
    downloadTorrents(engine)

    return


if __name__ == '__main__':
    main()