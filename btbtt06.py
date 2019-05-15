import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime



"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"

from btbtt06Costant import scoreUrlFormat
from btbtt06Costant import forumPageFormat
from btbtt06Costant import threadRegexp
from btbtt06Costant import attachementRegexp
from btbtt06Costant import attachementUrlFormat
from btbtt06Costant import attachementUrlRegexp
from btbtt06Costant import DB_URL
from btbtt06Costant import FID_IMG
from btbtt06Costant import REQUEST_HEADERS
from btbtt06Costant import NB_MAX_BLOCKED
from btbtt06Costant import FATCH_SIZE
from btbtt06Costant import STATUS_UNKNOW_ERROR

from btbtt06DAO import Base
from btbtt06DAO import ImageThread
from btbtt06DAO import Attachement
from btbtt06DAO import AttachementHeader
from btbtt06DAO import Link
from btbtt06DAO import Thread
from btbtt06DAO import ThreadHeader
from btbtt06DAO import ThreadHeaderInfo
from btbtt06DAO import Score


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


def getAttachementLinkFromRequest(link: str):
    import re
    req = requests.get(link, headers=REQUEST_HEADERS)
    if int(req.status_code) >= 400:
        return None

    body = req.json().get('message').get('body')
    soup: BeautifulSoup = BeautifulSoup(body, 'html.parser')

    attrLink = None

    aTags: [Tag] = soup.findAll(name='a')
    for aTag in aTags:
        href = aTag.attrs.get('href')
        if not href:
            continue
        if re.match(attachementUrlRegexp,href):
            attrLink = href
            break

    return attrLink


def getAttachementHeaderFromRequest(header: AttachementHeader):
    import re
    start = datetime.now().timestamp()
    if re.match(attachementRegexp,header.source):
        link = None
        try:
            link = getAttachementLinkFromRequest(header.source)
        except:
            pass
        if link:
            header.link = link
            print('Trans attachement link from {} to {} took {:.3f}(sec)'.format(
                header.source, header.link, datetime.now().timestamp()-start))
    return header


def downloadAttachement(header: AttachementHeader) -> dict:
    import re
    import base64
    from hashlib import sha256

    if not header:
        return None

    downloadUrl = header.link
    start = datetime.now()
    req = requests.get(downloadUrl, headers=REQUEST_HEADERS, timeout=(30, 600))

    status = int(req.status_code)

    if status == 404:
        header = getAttachementHeaderFromRequest(header)

    attachement = None
    if status < 400:
        attachement = Attachement()
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
    header.status = status
    header.comment = ""

    print("Download attachement from {} with status {} took {:.3f}(sec)".format(
        downloadUrl, status, header.mod_date.timestamp() - start.timestamp()))
    return {'header': header, 'attachement': attachement}


def makeAttachementHeader(link: Link, thread:str) -> Attachement:
    import re

    match = re.match(attachementRegexp, link.link)
    if not match:
        return None

    fid = match.group('fid')
    aid = match.group('aid')
    downloadUrl = attachementUrlFormat.format(fid, aid)

    attachement = AttachementHeader()
    attachement.thread = thread
    attachement.source = link.link
    attachement.link = downloadUrl
    attachement.aid = aid
    attachement.fid = fid
    attachement.title = link.title
    attachement.mod_date = datetime.now()

    print("Made empty attachement {}".format(attachement))

    return attachement


def makeImageHeader(link: Link, thread:str) -> Attachement:
    import re

    fid = FID_IMG
    aid = link.link

    attachement = AttachementHeader()
    attachement.thread = thread
    attachement.source = link.link
    attachement.link = link.link
    attachement.aid = aid
    attachement.fid = fid
    attachement.title = link.title
    attachement.mod_date = datetime.now()

    print("Made empty image {}".format(attachement))

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


def getImageAttachementHeaders(content:str,source:str)->dict:
    soup: BeautifulSoup = BeautifulSoup(content, 'html.parser')
    imgTags: [Tag] = soup.findAll(name='img')  # 使用属性字典方式
    attachementHeaders: [AttachementHeader] = []
    imageThreads:[ImageThread] = []
    for imgTag in imgTags:
        link = Link()
        link.link = imgTag.attrs.get('src')
        if not link.link:
            continue
        link.source = source
        link.title = imgTag.attrs.get('title')
        if not link.title:
            link.title = imgTag.text
        link.mod_date = datetime.now()
        attachementHeader = makeImageHeader(link,source)
        if attachementHeader:
            attachementHeaders.append(attachementHeader)
        imageThread = ImageThread()
        imageThread.thread = source
        imageThread.link = link.link
        imageThread.mod_date = datetime.now()
        imageThreads.append(imageThread)

    return {'headers':attachementHeaders,'imageThreads':imageThreads}


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
        attachement = makeAttachementHeader(attLink,source)
        if attachement:
            attachements.append(attachement)

    images = getImageAttachementHeaders(thread.info,source)
    attachements.extend(images.get('headers'))

    if not index and (thread.info is None or len(thread.info)) <= 40:
        print("Skip this page as desc too short")
        print(thread.info)
        links = []
        page = None
    print("Has {} links took {:.3f}(sec)".format(len(links), datetime.now().timestamp() - start))

    return {'thread': thread, 'links': links, 'attachements': attachements, 'imageThreads': images.get('imageThreads')}


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
        saveAll(atts, engine,merge=True)
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


def fetchAllThreads(dbUrl, threadFilter=None):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    nbResOld = -1
    nbBlocked = 0

    processBegin = datetime.now()
    if threadFilter is None:
        threadFilter = ((Thread.mod_date < processBegin)& (
                        (ThreadHeader.lastpost > Thread.mod_date) |
                        (Thread.link.is_(None))) )

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(ThreadHeader).outerjoin(Thread, Thread.link == ThreadHeader.link) \
                .filter(threadFilter) \
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

            print("{} Got {} headers took {:.3f}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start, nbResOld, nbBlocked,
                index, threadHeader.title, threadHeader.link))
            nbResOld = nbRes

            thread = readThread(threadHeader.link)
            savePage(thread, engine)
        except Exception as e:
            print("Error download thread for {}".format(e))


def fetchAllAttachements(dbUrl, attachementFilter=None):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    processBegin = datetime.now()

    if attachementFilter is None:
        attachementFilter = (
            (AttachementHeader.fid != FID_IMG) &
            ((AttachementHeader.downloaded.is_(None))|(AttachementHeader.downloaded<processBegin)) &
            ( AttachementHeader.status.is_(None) |
              ((AttachementHeader.status>=400) & (AttachementHeader.status < STATUS_UNKNOW_ERROR))
            )
        )

    nbResOld = -1
    nbBlocked = 0

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(AttachementHeader) \
                .filter(attachementFilter) \
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

            print("{} Got {} attachements took {:.3f}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start,
                nbResOld, nbBlocked, index, header.title, header.link))
            nbResOld = nbRes

            attachement = None
            try:
                attachementRes = downloadAttachement(header)
                attachement = attachementRes.get('attachement')
                header = attachementRes.get('header')
            except Exception as e:
                print("Skip download thread for {}".format(e))
                header.downloaded = datetime.now()
                header.status = STATUS_UNKNOW_ERROR
                header.comment = str(e)
                header.mod_date = datetime.now()

            session = Session()
            if header is not None:
                session.merge(header)
            if attachement is not None:
                session.merge(attachement)
            session.commit()
        except Exception as e:
            print("Error download attachement for {}".format(e))


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


def makeImageAttachement(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    start = datetime.now().timestamp()
    session = Session()
    from sqlalchemy.sql import exists, or_
    # results = session.query(Attachement.aid,Attachement.fid)\
    #     .filter(~exists().where(Attachement.link == AttachementHeader.link)).all()
    results = session.query(Thread.link).all()
    session.expunge_all()
    session.commit()

    nb = len(results)
    index = 0
    print("Have {} threads".format(nb))

    for threadLink in results:
        session = Session()
        thread = session.query(Thread) \
            .filter(Thread.link == threadLink[0]).first()
        session.expunge_all()
        session.commit()

        index += 1
        print("{}/{} {} - {}".format(index, nb, thread.title, thread.link))

        images = getImageAttachementHeaders(thread.info, thread.link)
        saveAll(images.get('headers'),engine)
        saveAll(images.get('imageThreads'),engine)




def makeAttachementHeaders(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    start = datetime.now().timestamp()
    session = Session()
    from sqlalchemy.sql import exists, or_
    # results = session.query(Attachement.aid,Attachement.fid)\
    #     .filter(~exists().where(Attachement.link == AttachementHeader.link)).all()
    results = session.query(Attachement.aid, Attachement.fid).all()
    session.expunge_all()
    session.commit()

    nb = len(results)
    index = 0
    print("Have {} attachements".format(nb))

    for attachementId in results:
        session = Session()
        attachement = session.query(Attachement) \
            .filter((Attachement.aid == attachementId[0]) & (Attachement.fid == attachementId[1])).first()
        session.expunge_all()
        session.commit()

        index += 1
        print("{}/{} {} - {}".format(index, nb, attachement.title, attachement.link))

        header = AttachementHeader()
        header.mod_date = datetime.now()
        header.source = attachement.source
        header.fid = attachement.fid
        header.aid = attachement.aid
        header.title = attachement.title
        header.link = attachement.link
        header.downloaded = attachement.mod_date
        session = Session()
        session.merge(header)
        session.commit()


def getNews(dbUrl):
    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)

    headerBegin = datetime.now()
    getAndSaveForumPages(engine, fid='1', end=1)  # Movie

    getAndSaveForumPages(engine, fid='2', end=1)  # 720p
    getAndSaveForumPages(engine, fid='3', end=1)  # 1080p
    getAndSaveForumPages(engine, fid='4', end=1)  # 3D
    getAndSaveForumPages(engine, fid='5', end=1)  # BD-RAW

    # getForumPages(engine, fid='6', end=12)
    # getForumPages(engine, fid='7', end=12)
    getAndSaveForumPages(engine, fid='8', end=1)  # Pic
    getAndSaveForumPages(engine, fid='9', end=1)  # X
    getAndSaveForumPages(engine, fid='10', end=1) # TVShow

    from btbtt06Costant import FID_TV_SHOW
    processBegin = datetime.now()
    threadFilter = ((ThreadHeader.mod_date < processBegin) & (
            (ThreadHeader.lastpost > Thread.mod_date) |
            ((ThreadHeader.fid == FID_TV_SHOW) & (ThreadHeader.mod_date > headerBegin)) |
            (Thread.link.is_(None))))

    fetchAllThreads(dbUrl,threadFilter=threadFilter)

    processBegin = datetime.now()
    attachementFilter = (
            ((AttachementHeader.downloaded.is_(None)) | (AttachementHeader.downloaded < processBegin)) &
            (AttachementHeader.status.is_(None) |
             ((AttachementHeader.status >= 400) & (AttachementHeader.status < STATUS_UNKNOW_ERROR))
            )
    )

    fetchAllAttachements(dbUrl,attachementFilter=attachementFilter)


def main():
    print("Start at {}".format(datetime.now().isoformat()))
    # fatchAllForumPages(DB_URL)
    # fetchAllThreads(DB_URL)
    # fetchAllAttachements(DB_URL)
    # getAllScores(engine)
    # getNews(engine)
    # thread = readThread('http://www.btbtt06.com/thread-index-fid-3-tid-8609.htm')
    # makeUnreadedAttachement(DB_URL)
    getNews(DB_URL)
    # makeImageAttachement(DB_URL)
    # makeAttachementHeaders(DB_URL)

    # link = Link()
    # link.link = 'http://www.btbttpic.com/upload/attach/000/021/96818034993e88263dd9b272868bb7fd.jpg'
    # header = makeImageHeader(link,"")
    # att = downloadAttachement(header)

    print("End at {}".format(datetime.now().isoformat()))
    return


if __name__ == '__main__':
    main()
