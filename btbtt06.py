import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime

Base = declarative_base()

"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"
scoreUrlFormat = "https://www.meijutt.com/inc/ajax.asp?id={}&action=newstarscorevideo"

forumPageFormat = 'http://www.btbtt06.com/forum-index-fid-{}-page-{}.htm'
threadRegexp = 'http://www.btbtt06.com/thread-index-fid-(?P<fid>[0-9]+)-tid-(?P<tid>[0-9]+).htm'
attachementRegexp = 'http://www.btbtt06.com/attach-dialog-fid-(?P<fid>[0-9]+)-aid-(?P<aid>[0-9]+)-ajax-1.htm'
attachementUrlFormat = "http://www.btbtt06.com/attach-download-fid-{}-aid-{}.htm"
DB_URL = 'sqlite:///./data/btbtt06.com.db'

REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
}

NB_MAX_BLOCKED = 10
FATCH_SIZE = 200


class Attachement(Base):
    # 表的名字:
    __tablename__ = 'ATTACHEMENT'

    def __init__(self):
        pass

    # 表的结构:
    hash = Column(String)  # VDPP,
    title = Column(String)  # VDPP,
    content = Column(String)  # observation,
    source = Column(String)  # observation,
    link = Column(String)  # observation,
    fid = Column(String, primary_key=True)
    aid = Column(String, primary_key=True)
    mod_date = Column(DateTime)

    def __str__(self) -> str:
        return "title: {}, fid: {}, aid: {}, link: {}, hash: {}".format(
            self.title, self.fid, self.aid, self.link, self.hash)


class AttachementHeader(Base):
    # 表的名字:
    __tablename__ = 'ATTACHEMENT_HEADER'

    def __init__(self):
        pass

    # 表的结构:
    downloaded = Column(DateTime)  # VDPP,
    title = Column(String)  # VDPP,
    source = Column(String)  # observation,
    link = Column(String)  # observation,
    fid = Column(String, primary_key=True)
    aid = Column(String, primary_key=True)
    mod_date = Column(DateTime)

    def __str__(self) -> str:
        return "title: {}, fid: {}, aid: {}, link: {}".format(
            self.title, self.fid, self.aid, self.link)


class Link(Base):
    # 表的名字:
    __tablename__ = 'LINK'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  # VDPP,
    title = Column(String)  # observation,
    source = Column(String)  # observation,
    mod_date = Column(DateTime)


class Thread(Base):
    # 表的名字:
    __tablename__ = 'THREAD'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  # VDPP,
    title = Column(String)  # observation,
    tid = Column(String)
    fid = Column(String)
    info = Column(String)  # observation,
    des = Column(String)  # observation,
    mod_date = Column(DateTime)


class ThreadHeader(Base):
    # 表的名字:
    __tablename__ = 'THREAD_HEADER'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  # VDPP,
    title = Column(String)  # observation,
    tid = Column(String)
    fid = Column(String)
    lastpost = Column(DateTime)
    mod_date = Column(DateTime)


class ThreadHeaderInfo(Base):
    # 表的名字:
    __tablename__ = 'THREAD_HEADER_INFO'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  # VDPP,
    thread = Column(String, primary_key=True)  # observation,
    mod_date = Column(DateTime)


class Score(Base):
    # 表的名字:
    __tablename__ = 'SCORE'

    def __init__(self):
        pass

    # 表的结构:
    source = Column(String, primary_key=True)  # VDPP,
    s1 = Column(Integer)
    s2 = Column(Integer)
    s3 = Column(Integer)
    s4 = Column(Integer)
    s5 = Column(Integer)
    total = Column(Integer)
    mean = Column(Float)
    mod_date = Column(DateTime)


def getThread(soup: BeautifulSoup, source: str):
    title = None
    try:
        title = soup.title.text
    except:
        pass

    metadata = None
    try:
        metadata = str(soup.select_one('div>.message'))
    except:
        pass

    des = None

    page = Thread()
    page.link = source
    page.mod_date = datetime.now()
    page.title = title
    page.des = des
    page.info = metadata
    return page


def getLinks(soup: BeautifulSoup, source: str):
    aTags: [Tag] = soup.findAll(name='a')  # 使用属性字典方式
    links: [Link] = []
    for aTag in aTags:
        link = Link()
        link.link = aTag.attrs.get('href')
        if not link.link:
            continue
        link.source = source
        link.title = aTag.attrs.get('title')
        if link.title is None:
            link.title = aTag.text
        link.mod_date = datetime.now()
        links.append(link)
    return links


def findAttaches(links: [Link]) -> [Link]:
    import re
    atts = []
    for link in links:
        try:
            match = re.match(attachementRegexp, link.link)
            if not match:
                continue
            atts.append(link)
        except:
            pass
    return atts


def downloadAttachement(header: AttachementHeader) -> dict:
    import re
    import base64
    from hashlib import sha256

    if not header:
        return None

    downloadUrl = header.link
    attachement = Attachement()
    start = datetime.now()
    req = requests.get(downloadUrl, headers=REQUEST_HEADERS)
    attachement.content = str(base64.b64encode(req.content), 'utf-8')
    attachement.hash = sha256(req.content).hexdigest()
    attachement.link = downloadUrl
    attachement.mod_date = datetime.now()
    attachement.title = header.title
    attachement.aid = header.aid
    attachement.fid = header.fid
    attachement.source = header.source

    header.mod_date = datetime.now()
    header.downloaded = datetime.now()

    print("Download attachement from {} took {}(sec)".format(downloadUrl,
                                                             start.timestamp() - attachement.mod_date.timestamp()))
    return {'header': header, 'attachement': attachement}


def makeAttachementHeader(link: Link) -> Attachement:
    import re

    match = re.match(attachementRegexp, link.link)
    if not match:
        return None

    fid = match.group('fid')
    aid = match.group('aid')
    downloadUrl = attachementUrlFormat.format(fid, aid)

    attachement = AttachementHeader()
    attachement.source = link.link
    attachement.link = downloadUrl
    attachement.aid = aid
    attachement.fid = fid
    attachement.title = link.title
    attachement.mod_date = datetime.now()

    print("Made empty attachement {}".format(attachement))

    return attachement


def getMoreLinks(content):
    import re
    if not content:
        return set()

    torrents = set()
    # matches = re.findall('(http://adh.hjzlg.com/img(.+?)\.torrent)',content)
    matches = re.findall('(ed2k://\|(.+?)\|/)', content)
    for match in matches:
        torrents.add(matches[0])

    matches = re.findall('(magnet:\?xt=urn:[a-zA-Z0-9]+:[\x21-\x7e]{32,})', content)
    for match in matches:
        torrents.add(matches[0])

    return torrents


def readThread(source, index=False) -> dict:
    import re
    videoId = re.match(threadRegexp, source)
    start = datetime.now().timestamp()

    if not videoId:
        return None

    fid = videoId.group('fid')
    tid = videoId.group('tid')
    # print("{} Get thread fid {} tid {}".format(datetime.now().isoformat(),fid,tid))

    ret = requests.get(url=source, headers=REQUEST_HEADERS, timeout=(30, 30))
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    soup: BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快

    thread = getThread(soup, source)
    if thread:
        thread.fid = fid
        thread.tid = tid

    links = getLinks(soup, source)
    moreLinks = getMoreLinks(thread.info)
    for torrent in moreLinks:
        link = Link()
        link.source = source
        link.link = torrent
        link.mod_date = datetime.now()
        links.append(link)

    attLinks = findAttaches(links)

    attachements = []
    for attLink in attLinks:
        attachement = makeAttachementHeader(attLink)
        if attachement:
            attachements.append(attachement)

    if not index and (thread.info is None or len(thread.info)) <= 40:
        print("Skip this page as desc too short")
        print(thread.info)
        links = []
        page = None
    print("Has {} links took {}(sec)".format(len(links), datetime.now().timestamp() - start))

    return {'thread': thread, 'links': links, 'attachements': attachements}


def saveAll(objs, engine, merge=False):
    Session = sessionmaker(bind=engine)
    count = 0
    for link in objs:
        try:
            session = Session()
            if merge:
                session.merge(link)
            else:
                session.add(link)
            session.commit()
            count += 1
        except IntegrityError as e:
            pass


def savePage(page: dict, engine):
    Session = sessionmaker(bind=engine)
    try:
        session = Session()
        pageDAO = page.get('thread')
        session.merge(pageDAO)
        session.commit()
    except Exception as e:
        print("Error save page {} with {}".format(page, str(e)))

    try:
        links = page.get('links')
        saveAll(links, engine)
    except Exception as e:
        print("Error save links {} with {}".format(page, str(e)))

    try:
        atts = page.get('attachements')
        saveAll(atts, engine)
    except Exception as e:
        print("Error save atts {} with {}".format(page, str(e)))


def getThreadList(source) -> dict:
    import re

    ret = requests.get(url=source, headers=REQUEST_HEADERS, timeout=(30, 30))
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    soup: BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快
    links = getLinks(soup, source)

    threadLinks = set()
    for link in links:
        threadLinks.add(link)

    threadTables = soup.select('table.thread')

    threadHeaders = []
    for threadTable in threadTables:
        threadHeaders.append(readThreadHeader(threadTable))

    return {'links': threadLinks, 'headers': threadHeaders}


def readThreadHeader(table: Tag) -> dict:
    import re

    lastpostStr = table.attrs.get('lastpost')
    lastpost = None

    try:
        lastpost = datetime.fromtimestamp(float(lastpostStr))
    except:
        pass

    tid = table.attrs.get('tid')

    subjectATag: Tag = table.select_one('a.subject_link')

    threadHeader = None
    if subjectATag:
        title = subjectATag.text
        link = subjectATag.attrs.get('href')
        match = re.match(threadRegexp, link)
        fid = None
        if match:
            fid = match.group('fid')

        threadHeader = ThreadHeader()
        threadHeader.link = link
        threadHeader.tid = tid
        threadHeader.fid = fid
        threadHeader.title = title
        threadHeader.mod_date = datetime.now()
        threadHeader.lastpost = lastpost

    threadHeaderInfos = []
    if threadHeader:
        infoATags = table.select('a.subject_type')
        for aTag in infoATags:
            info = ThreadHeaderInfo()
            info.mod_date = datetime.now()
            info.link = aTag.attrs.get('href')
            info.thread = threadHeader.link
            threadHeaderInfos.append(info)

    return {'header': threadHeader, 'info': threadHeaderInfos}


def saveThreadHeaders(engine, threadHeaders: dict):
    Session = sessionmaker(bind=engine)
    try:
        links = threadHeaders.get('links')
        saveAll(links, engine)
    except Exception as e:
        print("Error save links {} with {}".format(threadHeaders, str(e)))

    headers = threadHeaders.get('headers')
    for header in headers:
        saveHeader(engine, header)


def saveHeader(engine, header: dict):
    Session = sessionmaker(bind=engine)
    try:
        session = Session()
        headerDAO = header.get('header')
        session.merge(headerDAO)
        session.commit()
    except Exception as e:
        print("Error save page {} with {}".format(header, str(e)))

    try:
        infos = header.get('info')
        saveAll(infos, engine)
    except Exception as e:
        print("Error save links {} with {}".format(header, str(e)))


def getAndSaveForumPage(engine, source):
    threadHeaders = getThreadList(source)
    nbLinks = 0
    nbHeaders = 0
    try:
        nbLinks = len(threadHeaders.get('links'))
        nbHeaders = len(threadHeaders.get('headers'))
    except:
        pass
    print("Has {} links {} hraders".format(nbLinks, nbHeaders))
    saveThreadHeaders(engine, threadHeaders)


def getNews(engine):
    pass


def getAndSaveForumPages(engine, fid='1', start=1, end=1):
    end = end + 1
    for i in range(start, end):
        pageLink = forumPageFormat.format(fid, i)
        print("{} {}".format(datetime.now().isoformat(), pageLink))
        try:
            getAndSaveForumPage(engine, pageLink)
        except Exception as e:
            print("Error with {} for {}".format(pageLink, e))


def fetchAllForumPages(dbUrl):
    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    getAndSaveForumPages(engine, fid='1', end=268, start=32)
    getAndSaveForumPages(engine, fid='2', end=12)
    getAndSaveForumPages(engine, fid='3', end=35)
    getAndSaveForumPages(engine, fid='4', end=4)
    getAndSaveForumPages(engine, fid='5', end=19)
    # getForumPages(engine, fid='6', end=12)
    # getForumPages(engine, fid='7', end=12)
    getAndSaveForumPages(engine, fid='8', end=21)
    getAndSaveForumPages(engine, fid='9', end=33)
    getAndSaveForumPages(engine, fid='10', end=2)


def fetchAllThreads(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    nbResOld = -1
    nbBlocked = 0

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(ThreadHeader).outerjoin(Thread, Thread.link == ThreadHeader.link) \
                .filter((ThreadHeader.lastpost > Thread.mod_date) | (Thread.link.is_(None))) \
                .limit(FATCH_SIZE).all()
            session.expunge_all()
            session.commit()

            nbRes = len(results)
            if nbResOld == nbRes and nbRes != FATCH_SIZE:
                nbBlocked += 1
            else:
                nbBlocked = 0

            if len(results) == 0 or nbBlocked > NB_MAX_BLOCKED:
                break

            index = random.randrange(len(results))
            threadHeader: ThreadHeader = results[index]

            print("{} Got {} headers took {}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start, nbResOld, nbBlocked,
                index, threadHeader.title, threadHeader.link))
            nbResOld = nbRes

            thread = readThread(threadHeader.link)
            savePage(thread, engine)
        except Exception as e:
            print("Error download thread for {}".format(e))


def fetchAllAttachements(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    nbResOld = -1
    nbBlocked = 0

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(AttachementHeader) \
                .filter(AttachementHeader.downloaded.is_(None)) \
                .limit(FATCH_SIZE).all()
            session.expunge_all()
            session.commit()

            nbRes = len(results)
            if nbResOld == nbRes and nbRes != FATCH_SIZE:
                nbBlocked += 1
            else:
                nbBlocked = 0

            if len(results) == 0 or nbBlocked > NB_MAX_BLOCKED:
                break

            index = random.randrange(len(results))
            header: AttachementHeader = results[index]

            print("{} Got {} attachements took {}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start,
                nbResOld, nbBlocked, index, header.title, header.link))
            nbResOld = nbRes

            attachement = downloadAttachement(header)
            session = Session()
            session.merge(attachement.get('attachement'))
            session.merge(attachement.get('header'))
            session.commit()
        except ValueError as e:
            print("Error download thread for {}".format(e))


def makeUnreadedAttachement(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    start = datetime.now().timestamp()
    session = Session()
    from sqlalchemy.sql import exists, or_
    results = session.query(Attachement) \
        .filter(Attachement.content.is_(None)) \
        .limit(5000).all()
    session.expunge_all()
    session.commit()

    print(len(results))

    for attachement in results:
        header = AttachementHeader()
        header.mod_date = datetime.now()
        header.source = attachement.source
        header.fid = attachement.fid
        header.aid = attachement.aid
        header.title = attachement.title
        header.link = attachement.link
        session = Session()
        session.merge(header)
        session.commit()


def main():
    print("Start at {}".format(datetime.now().isoformat()))
    # fatchAllForumPages(DB_URL)
    # fetchAllThreads(DB_URL)
    # fetchAllAttachements(DB_URL)
    # getAllScores(engine)
    # getNews(engine)
    # thread = readThread('http://www.btbtt06.com/thread-index-fid-3-tid-8609.htm')
    makeUnreadedAttachement(DB_URL)

    print("End at {}".format(datetime.now().isoformat()))
    return


if __name__ == '__main__':
    main()
