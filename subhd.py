from datetime import datetime

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from subhdDAO import Base
from subhdDAO import Sub
from subhdDAO import WorkItem

from subhdCostant import FATCH_SIZE
from subhdCostant import NB_MAX_BLOCKED

from subhdCostant import DB_URL
from subhdCostant import REQUEST_HEADERS
from subhdCostant import REGEXP_URL_FULL
from subhdCostant import REGEXP_AR
from subhdCostant import REGEXP_DO
from subhdCostant import REGEXP_ZU
from subhdCostant import REGEXP_USER
from subhdCostant import REGEXP_INDEX
from subhdCostant import REGEXP_DOUBAN

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime


proxies = {
  "http": "http://redqueen.lan.linyz.net:7890",
  "https": "http://redqueen.lan.linyz.net:7890",
}

reqSession = requests.Session()
reqSession.proxies = proxies



def makeFullLink(link):
    from subhdCostant import DOMAIN
    return '{}{}'.format(DOMAIN, link)


def getSub(link: str) -> Sub:
    import re

    start = datetime.now().timestamp()

    match = re.match(REGEXP_URL_FULL, link)
    if match:
        link = match.group('path')

    match = re.match(REGEXP_AR, link)
    if not match:
        return None

    sub = Sub()
    sub.link = link
    # ar
    sub.ar = match.group('id')

    fullLink = makeFullLink(link)
    ret = reqSession.get(url=fullLink, headers=REQUEST_HEADERS, timeout=(30, 30))
    sub.status = int(ret.status_code)
    if sub.status >= 400:
        sub.mod_date = datetime.now()
        return sub

    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    soup: BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快

    # index s ep title
    indexTag: Tag = soup.select_one('.movielabel')
    if not indexTag:
        indexTag = soup.select_one('.tvlabel')

    if indexTag:
        sub.index = indexTag.text.strip()
        sub.title = indexTag.parent.text
        if sub.title:
            match = re.match(REGEXP_INDEX, sub.title)
            if match:
                sub.season = int(match.group('s'))
                sub.episode = int(match.group('e'))

    if not sub.title:
        try:
            sub.title = soup.select_one('h1').text
        except:
            pass

    # do
    doATag: Tag = soup.select_one('.col-md-10 > h1:nth-child(1) > a:nth-child(1)')
    if doATag:
        sub.do_link = doATag.attrs.get('href')
        match = re.match(REGEXP_DO, sub.do_link)
        if match:
            sub.do = match.group('id')

    # douban
    doubanTag: Tag = soup.select_one('div.s_detail > a:nth-child(2)')
    if doubanTag:
        sub.douban_link = doubanTag.attrs.get('href')

    # team u
    memberTableTag: Tag = soup.select_one('table.numbers')
    aTags: [Tag] = memberTableTag.select('a')
    for aTag in aTags:
        href = aTag.attrs.get('href')
        zuMatch = re.match(REGEXP_ZU, href)
        if zuMatch:
            sub.team = zuMatch.group('id')
            sub.team_link = href

        userMatch = re.match(REGEXP_USER, href)
        if userMatch:
            sub.user = userMatch.group('id')
            sub.user_link = href

    # attachement dtoken sid
    attrTag: Tag = soup.select_one('#down')
    if attrTag:
        sub.dtoken = attrTag.attrs.get('dtoken')
        sub.sid = attrTag.attrs.get('sid')
    else:
        from subhdCostant import STATUS_SUB_FORBIDDEN_ERROR
        sub.status = STATUS_SUB_FORBIDDEN_ERROR

    # info des
    boxTitleTags: [Tag] = soup.select('div.box > div.b')
    import subhdCostant
    for boxTitleTag in boxTitleTags:
        if boxTitleTag.text == '字幕信息':
            info = boxTitleTag.parent.text
            sub.info = info
            match = re.search(subhdCostant.REGEXP_TYPE, info)
            if match:
                sub.type = match.group('text')
            match = re.search(subhdCostant.REGEXP_LANG, info)
            if match:
                sub.lang = match.group('text')
            match = re.search(subhdCostant.REGEXP_SOURCE, info)
            if match:
                sub.source = match.group('text')
            match = re.search(subhdCostant.REGEXP_BI_LANG, info)
            if match:
                sub.bi_lang = match.group('text')
            match = re.search(subhdCostant.REGEXP_FORMAT, info)
            if match:
                sub.format = match.group('text')
            match = re.search(subhdCostant.REGEXP_VERSION, info)
            if match:
                sub.version = match.group('text')
        if boxTitleTag.text == '字幕说明':
            sub.des = boxTitleTag.parent.text

    # sub.attachement_link = Column(String)
    # sub.attachement_path = Column(String)

    sub.mod_date = datetime.now()
    # sub.comment = Column(String)
    # sub.downloaded = Column(DateTime)
    print("Get sub header {} - {} with {} took {:.3f}".format(
        sub.title, sub.link, sub.status, datetime.now().timestamp() - start))

    return sub


def readWorkItem(soup: BeautifulSoup, link) -> WorkItem:
    import re

    match = re.match(REGEXP_DO, link)
    if not match:
        return None

    wordItem = WorkItem()
    wordItem.link = link
    # ar
    wordItem.do = match.group('id')

    # index s ep title
    infoTag: Tag = soup.select_one('.col-xs-10')

    if infoTag:
        wordItem.title = infoTag.select_one('h1').text
        wordItem.title_other = infoTag.select_one('h2').text

        # douban
        aTags: [Tag] = infoTag.select('a')
        for aTag in aTags:
            href = aTag.attrs.get('href')
            if re.match(REGEXP_DOUBAN, href):
                wordItem.douban_link = href

        info = infoTag.text
        import subhdCostant
        if info:
            wordItem.info = info

            match = re.search(subhdCostant.REGEXP_YEAR_AREA, info)
            if match:
                wordItem.year = match.group('year')
                wordItem.area = match.group('area')

            match = re.search(subhdCostant.REGEXP_ITEM_DIRECTOR, info)
            if match:
                wordItem.director = match.group('text')

            match = re.search(subhdCostant.REGEXP_ITEM_ACTOR, info)
            if match:
                wordItem.actor = match.group('text')

            match = re.search(subhdCostant.REGEXP_ITEM_TYPE, info)
            if match:
                wordItem.type = match.group('text')

            match = re.search(subhdCostant.REGEXP_ITEM_DES, info)
            if match:
                wordItem.des = match.group('text')

    # sub.attachement_link = Column(String)
    # sub.attachement_path = Column(String)

    wordItem.mod_date = datetime.now()
    # sub.status = Column(Integer)
    # sub.comment = Column(String)
    # sub.downloaded = Column(DateTime)

    return wordItem


def getWorkItem(link: str) -> dict:
    import re

    start = datetime.now().timestamp()

    match = re.match(REGEXP_URL_FULL, link)
    if match:
        link = match.group('path')

    match = re.match(REGEXP_DO, link)
    if not match:
        return None

    fullLink = makeFullLink(link)
    ret = reqSession.get(url=fullLink, headers=REQUEST_HEADERS, timeout=(30, 30))
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码

    if int(ret.status_code) >= 400:
        return None

    soup: BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快

    wordItem = readWorkItem(soup, link)

    links = set()
    aTags: [Tag] = soup.select('a')
    for aTag in aTags:
        href = aTag.attrs.get('href')
        if re.match(REGEXP_AR, href):
            links.add(href)

    wordItem.status = int(ret.status_code)
    wordItem.mod_date = datetime.now()

    print("Download item {} - {}".format(wordItem.title, wordItem.link))

    if len(links) == 0:
        return None

    return {'wordItem': wordItem, 'ar': links}


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


def makeEmptySub(link):
    import re
    match = re.match(REGEXP_URL_FULL, link)
    if match:
        link = match.group('path')

    match = re.match(REGEXP_AR, link)
    if not match:
        return None

    sub = Sub()
    sub.link = link
    sub.mod_date = datetime.now()
    return sub


def downloadItem(link, engine):
    item = getWorkItem(link)
    if not item:
        return

    itemDAO = item.get('wordItem')
    links = item.get('ar')
    ars: [Sub] = []
    for link in links:
        sub = makeEmptySub(link)
        if sub:
            ars.append(sub)

    Session = sessionmaker(bind=engine)
    try:
        session = Session()
        session.merge(itemDAO)
        session.commit()
    except:
        pass

    saveAll(ars, engine)

    return itemDAO


def getDownloadUrl(sid, dtoken):
    url = None
    try:
        data = {'sub_id': sid,
                'dtoken': dtoken}
        from subhdCostant import DOWN_URL
        # DOWN_URL = 'http://httpbin.org/post'

        ret = reqSession.post(DOWN_URL, data=data, headers=REQUEST_HEADERS, timeout=(30, 30))
        ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
        downInfo = ret.json()
        url = downInfo.get('url')
    except:
        pass
    return url


def downloadSub(sub: Sub, workItemTitle=None, outputFlag=False):
    import shutil
    import os.path
    from pathlib import Path
    from subhdCostant import ATTACHEMENT_BASE_PATH
    from subhdCostant import ATTACHEMENT_OUT_BASE_PATH

    existFlag = False
    subPath = sub.attachement_path
    if subPath is not None:
        subPath = Path(ATTACHEMENT_BASE_PATH).joinpath(subPath)
        if os.path.isfile(subPath):
            existFlag = True

    if not existFlag:
        downUrl = getDownloadUrl(sub.sid, sub.dtoken)
        # downUrl = 'http://dl1.subhd.com/sub/2019/03/155343934746869.zip'
        if not downUrl:
            return None
        sub.attachement_link = downUrl

        parts = Path(downUrl).parts[2:]  # cut http[s] and host
        subPath = Path("")
        for part in parts:
            subPath = subPath.joinpath(part)
        fullPath = Path(ATTACHEMENT_BASE_PATH).joinpath(subPath)
        os.makedirs(fullPath.parent, mode=0o755, exist_ok=True)

        ret = reqSession.get(downUrl, headers=REQUEST_HEADERS, timeout=(30, 300))
        sub.attachement_status = int(ret.status_code)

        if sub.attachement_status and sub.attachement_status < 400:
            with open(fullPath, "wb") as code:
                code.write(ret.content)

        sub.attachement_path = subPath.as_posix()
        sub.attachement_link = downUrl
        sub.downloaded = datetime.now()
        sub.mod_date = datetime.now()
        print("Download {} to {}".format(sub.title, sub.attachement_path))

    if outputFlag:
        try:
            extractSub(sub, workItemTitle)
        except ImportError as e:
            print("Error extract sub from {} - {} for {}".format(sub.title, sub.attachement_path, str(e)))

    return sub


def extractSub(sub: Sub, workItemTitle=None):
    import re
    import uuid
    import shutil
    import os.path
    import zipfile
    import rarfile
    from pathlib import Path

    from subhdCostant import ATTACHEMENT_BASE_PATH
    from subhdCostant import ATTACHEMENT_OUT_BASE_PATH
    fullPath = Path(ATTACHEMENT_OUT_BASE_PATH)

    if workItemTitle:
        subdir = re.sub(r'[ <>*?|:/\\\t"]', '_', workItemTitle)
        fullPath = fullPath.joinpath(subdir)

    subdir = sub.title
    if not subdir:
        subdir = "UNKNOWN_FILE_{}".format(str(uuid.uuid4()))

    subdir = re.sub(r'[ <>*?|:/\\\t"]', '_', subdir)
    fullPath = fullPath.joinpath(subdir)
    os.makedirs(fullPath, mode=0o755, exist_ok=True)

    existFlag = False
    subPath = sub.attachement_path
    if subPath is not None:
        subPath = Path(ATTACHEMENT_BASE_PATH).joinpath(subPath)
        if os.path.isfile(subPath):
            existFlag = True

    if not existFlag:
        print("Achive file not found {}".format(subPath))
        return

    if re.match('.*\.rar', subPath.as_posix()):
        try:
            with rarfile.RarFile(str(subPath)) as achive:
                achive.extractall(str(fullPath))
                print("extract rar {} to {}".format(subPath.as_posix(), fullPath.as_posix()))
        except Exception as e:
            print(e)
            srcPath = subPath
            dstPath = fullPath.joinpath(subPath.name)
            shutil.copyfile(srcPath, dstPath)
            print("Copy {} to {}".format(subPath.as_posix(), dstPath.as_posix()))
    else:
        srcPath = subPath
        dstPath = fullPath.joinpath(subPath.name)
        shutil.copyfile(srcPath, dstPath)
        print("Copy {} to {}".format(subPath.as_posix(), dstPath.as_posix()))

        if re.match('.*\.zip', subPath.as_posix()):
            try:
                with zipfile.ZipFile(subPath) as achive:
                    achive.extractall(fullPath)
                    print("extract zip {} to {}".format(subPath.as_posix(), fullPath.as_posix()))
            except Exception as e:
                print(e)


def fatchItem(link, dbUrl):
    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    item = downloadItem(link, engine)

    return item


def fatchFromFeed(feedUrl, dbUrl):
    import re
    print("Fatch headers from feed {}".format(feedUrl))
    ret = reqSession.get(feedUrl)
    ret.encoding = ret.apparent_encoding  # 指定编码等于原始页面编码
    soup: BeautifulSoup = BeautifulSoup(ret.text, 'html.parser')  # 使用lxml则速度更快

    subs = []
    linkTags = soup.select('item')
    for itemTag in linkTags:
        text = itemTag.text
        match = re.search(REGEXP_AR, text)
        if match:
            link = match.group(0)
            sub = makeEmptySub(link)
            if sub:
                subs.append(sub)

    engine = create_engine(dbUrl)
    saveAll(subs, engine)


def fetchAllSubHeaders(dbUrl, attrfilter=None):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    nbResOld = -1
    nbBlocked = 0

    processBegin = datetime.now()

    if attrfilter is None:
        attrfilter = ((Sub.mod_date < processBegin) &
                      ((Sub.status.is_(None)) | (Sub.status.between(400, 999))))

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(Sub) \
                .filter(attrfilter) \
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
            sub: Sub = results[index]

            print("{} Got {} subs took {:.3f}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start, nbResOld, nbBlocked,
                index, sub.title, sub.link))
            nbResOld = nbRes

            try:
                sub = getSub(sub.link)
            except Exception as e:
                from subhdCostant import STATUS_UNKNOW_ERROR
                sub.status = STATUS_UNKNOW_ERROR
                sub.comment = str(e)
                sub.mod_date = datetime.now()

            session = Session()
            if sub:
                session.merge(sub)
            session.commit()
        except Exception as e:
            print("Error download thread for {}".format(e))


def fetchSub(link, engine, workItemTitle=None, outputFlag=False):
    Session = sessionmaker(bind=engine)
    session = Session()
    sub: Sub = session.query(Sub) \
        .filter((Sub.link == link)) \
        .first()
    session.expunge_all()
    session.commit()

    sub = downloadSub(sub, workItemTitle, outputFlag)
    session = Session()
    if sub:
        session.merge(sub)
    session.commit()


def fetchFeedAndHeaders(dbUrl):
    from subhdCostant import FEED_URL

    fatchFromFeed(FEED_URL, dbUrl)
    fetchAllSubHeaders(dbUrl)
    fetchSubAll(dbUrl)


def fetchSubThread(link, dbUrl, outputFlag=False):
    processBegin = datetime.now()

    item: WorkItem = fatchItem(link, dbUrl)

    # filter = ((Sub.mod_date < processBegin) & # (Sub.do_link == link) &
    #           ((Sub.status.is_(None)) | (Sub.status.between(400, 999))))
    fetchAllSubHeaders(dbUrl)

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    subs = session.query(Sub.link) \
        .filter((Sub.do_link == link)) \
        .all()
    session.expunge_all()
    session.commit()

    for sub in subs:
        workItemTitle = None
        if item:
            workItemTitle = "{}-{}".format(item.title, item.do)
        fetchSub(sub[0], engine, workItemTitle, outputFlag)


def fetchSubArLink(link, dbUrl, outputFlag=False):
    engine = create_engine(dbUrl)
    fetchSub(link, engine, None, outputFlag)


def fetchSubAll(dbUrl, attrfilter=None):
    import random
    from datetime import timedelta

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    nbResOld = -1
    nbBlocked = 0

    processBegin = datetime.now()

    if attrfilter is None:
        attrfilter = ((Sub.mod_date > (datetime.now()-timedelta(days=7))) &
                      (Sub.mod_date < processBegin) & (Sub.status <= 400) &
                      ((Sub.attachement_status.is_(None)) |
                      (Sub.attachement_status.between(400, 999)))
                      )

    while True:
        try:
            start = datetime.now().timestamp()
            session = Session()
            from sqlalchemy.sql import exists, or_
            results = session.query(Sub) \
                .filter(attrfilter) \
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
            sub: Sub = results[index]

            print("{} Got {} subs took {:.3f}(sec) old is {} blocked {}, select nb {}: {} - {}".format(
                datetime.now().isoformat(), nbRes, datetime.now().timestamp() - start, nbResOld, nbBlocked,
                index, sub.title, sub.link))
            nbResOld = nbRes

            try:
                sub = downloadSub(sub, None, False)
            except Exception as e:
                from subhdCostant import STATUS_UNKNOW_ERROR
                sub.status = STATUS_UNKNOW_ERROR
                sub.comment = str(e)
                sub.mod_date = datetime.now()

            session = Session()
            if sub:
                session.merge(sub)
            session.commit()
        except Exception as e:
            print("Error download thread for {}".format(e))


if __name__ == '__main__':
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    # fetchSub('/ar0/488564', engine)

    # fetchSubThread('/do0/27010768', DB_URL, outputFlag=True)
    fetchSubThread('/do0/30163504', DB_URL, outputFlag=True)
    # fetchSubThread('/do0/27119724', DB_URL, outputFlag=True)
    # fetchSubThread('/do0/30163504', DB_URL, outputFlag=True)
    fetchSubThread('/do0/30454230', DB_URL, outputFlag=True)

    # attachement = getSub('/ar0/487113')
    # item = getWorkItem('https://subhd.com/do0/30424374')
    # getDownloadUrl('494831','30688bd5d1efc70995ef7a072dc4f30ddac6ea4b')

    # fatchItem('https://subhd.com/do0/30424374', DB_URL)

    # fetchFeedAndHeaders(DB_URL)

    pass
