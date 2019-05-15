from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime

Base = declarative_base()

class ImageThread(Base):
    # 表的名字:
    __tablename__ = 'IMAGE_THREAD'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)  # observation,
    thread = Column(String, primary_key=True)  # observation,
    mod_date = Column(DateTime)


class Attachement(Base):
    # 表的名字:
    __tablename__ = 'ATTACHEMENT'

    def __init__(self):
        pass

    # 表的结构:
    hash = Column(String)
    title = Column(String)
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
    downloaded = Column(DateTime)
    title = Column(String)
    source = Column(String)  # observation,
    link = Column(String)  # observation,
    fid = Column(String, primary_key=True)
    aid = Column(String, primary_key=True)
    mod_date = Column(DateTime)
    status = Column(Integer)
    comment = Column(String)
    thread = Column(String)

    def __str__(self) -> str:
        return "title: {}, fid: {}, aid: {}, link: {}".format(
            self.title, self.fid, self.aid, self.link)


class Link(Base):
    # 表的名字:
    __tablename__ = 'LINK'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)
    title = Column(String)  # observation,
    source = Column(String)  # observation,
    mod_date = Column(DateTime)


class Thread(Base):
    # 表的名字:
    __tablename__ = 'THREAD'

    def __init__(self):
        pass

    # 表的结构:
    link = Column(String, primary_key=True)
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
    link = Column(String, primary_key=True)
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
    link = Column(String, primary_key=True)
    thread = Column(String, primary_key=True)  # observation,
    mod_date = Column(DateTime)


class Score(Base):
    # 表的名字:
    __tablename__ = 'SCORE'

    def __init__(self):
        pass

    # 表的结构:
    source = Column(String, primary_key=True)
    s1 = Column(Integer)
    s2 = Column(Integer)
    s3 = Column(Integer)
    s4 = Column(Integer)
    s5 = Column(Integer)
    total = Column(Integer)
    mean = Column(Float)
    mod_date = Column(DateTime)