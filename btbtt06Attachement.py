from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime


from btbtt06DAO import AttachementHeader as AttachementHeader
from btbtt06DAO import Base as Base
from btbtt06DAO import Attachement as Attachement
from btbtt06DAO import Thread

from btbtt06Costant import FID_IMG
from btbtt06Costant import FATCH_SIZE
from btbtt06Costant import NB_MAX_BLOCKED
from btbtt06Costant import DB_URL

from btbtt06 import fetchAllAttachements


def saveAttachement(attachement: Attachement, path="./data/attachements", subdir=None):
    import base64
    from pathlib import Path
    import os
    import uuid
    import re

    dirPath = Path(path)

    if subdir:
        subdir = re.sub(r'[ <>*?|:/\\"]', "_", subdir)
        dirPath = dirPath.joinpath(subdir)

    os.makedirs(dirPath, mode=0o755, exist_ok=True)

    fileb64 = attachement.content

    fileName = attachement.title

    if not fileName:
        fileName = Path(attachement.link).name

    if not fileName:
        fileName = "UNKNOWN_FILE_{}".format(str(uuid.uuid4()))

    filePath = dirPath.joinpath(fileName)

    print("Saving {}".format(filePath))

    fh = open(filePath, "wb")
    fh.write(base64.b64decode(fileb64))
    fh.close()


def getAttachementFromDB(attrFilter, dbUrl, subdir=None):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    session = Session()
    from sqlalchemy.sql import exists, or_

    attachementHeaders: [AttachementHeader] = session.query(AttachementHeader).filter(
        attrFilter
    ).limit(FATCH_SIZE).all()
    session.expunge_all()
    session.commit()

    print(len(attachementHeaders))

    for header in attachementHeaders:
        session = Session()
        from sqlalchemy.sql import exists, or_

        attachement = (
            session.query(Attachement)
            .filter((Attachement.fid == header.fid) & (Attachement.aid == header.aid))
            .first()
        )
        session.expunge_all()
        session.commit()

        try:
            saveAttachement(attachement, subdir=subdir)
        except Exception as e:
            print(
                "Save {} - {} failed with {}".format(header.title, header.link, str(e))
            )


def getAttachementThread(dbUrl, threadLink: str):
    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    session = Session()
    from sqlalchemy.sql import exists, or_

    thread: Thread = session.query(Thread).filter(Thread.link == threadLink).limit(
        FATCH_SIZE
    ).first()
    session.expunge_all()
    session.commit()

    attrfilter = AttachementHeader.thread == threadLink
    getAttachementFromDB(attrfilter, DB_URL, subdir=thread.title)


def getAttachement():
    links = [
        "http://www.btbtt06.com/attach-download-fid-10-aid-99156.htm",
        "http://www.btbtt06.com/attach-download-fid-10-aid-99370.htm",
        "http://www.btbttpic.com/upload/attach/000/042/82c5bdd8759803244e9d63d311332d53.jpg",
    ]

    attrfilter = AttachementHeader.link.in_(links)

    getAttachementFromDB(attrfilter, DB_URL)


if __name__ == "__main__":
    # getAttachementThread(DB_URL, 'http://www.btbtt06.com/thread-index-fid-1-tid-4410.htm')
    getAttachementThread(
        DB_URL, "http://www.btbtt06.com/thread-index-fid-1-tid-15351.htm"
    )

