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


def fetchAllImages(dbUrl):
    nbResOld = -1
    nbBlocked = 0

    attrfilter = (
                    (AttachementHeader.fid == FID_IMG) &
                    (AttachementHeader.downloaded.is_(None)) &
                    (AttachementHeader.link.like('https://www.btbttpic.com/upload/attach%.jpg'))
    )

    attrfilter = (
                    (AttachementHeader.fid == FID_IMG) &
                    (AttachementHeader.downloaded.is_(None)) &
                    (~AttachementHeader.link.like('http://i2.tietuku.cn/%'))
    )


    fetchAllAttachements(dbUrl,attrfilter)


if __name__ == '__main__':
    fetchAllImages(DB_URL)
