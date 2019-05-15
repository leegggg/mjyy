from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime


from btbtt06DAO import AttachementHeader as AttachementHeader
from btbtt06DAO import Base as Base
from btbtt06DAO import Attachement as Attachement

from btbtt06Costant import FID_IMG
from btbtt06Costant import FATCH_SIZE
from btbtt06Costant import NB_MAX_BLOCKED
from btbtt06Costant import DB_URL

from btbtt06 import fetchAllAttachements


def updateImgHeader(dbUrl):
    import random

    engine = create_engine(dbUrl)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    start = datetime.now().timestamp()
    session = Session()
    from sqlalchemy.sql import exists, or_
    # results = session.query(Attachement.aid,Attachement.fid)\
    #     .filter(~exists().where(Attachement.link == AttachementHeader.link)).all()
    results = session.query(Attachement.aid, Attachement.fid)\
        .filter(Attachement.link.like('http://www.btbttpic.com/upload/%')).all()
    session.expunge_all()
    session.commit()

    nb = len(results)
    index = 0
    print("Have {} attachements".format(nb))

    for attachementId in results:
        session = Session()
        header = session.query(AttachementHeader) \
            .filter((AttachementHeader.aid == attachementId[0]) & (AttachementHeader.fid == attachementId[1])).first()
        session.expunge_all()
        session.commit()

        index += 1
        print("{}/{} {} - {}".format(index, nb, header.title, header.link))

        header.mod_date = datetime.now()
        header.status = 200
        session = Session()
        session.merge(header)
        session.commit()



def fetchAllImages(dbUrl):
    nbResOld = -1
    nbBlocked = 0

    attrfilter = (
                    (AttachementHeader.fid == FID_IMG) &
                    (AttachementHeader.downloaded.is_(None)) &
                    (AttachementHeader.link.like('https://www.btbttpic.com/upload/attach%.jpg'))
    )

    attrfilter = (
                    (AttachementHeader.fid == FID_IMG)
                    & (AttachementHeader.status.is_(None))
                    # & (~AttachementHeader.link.like('http://i2.tietuku.cn/%'))
    )

    fetchAllAttachements(dbUrl,attrfilter)


if __name__ == '__main__':
    fetchAllImages(DB_URL)
    # updateImgHeader(DB_URL)
