from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from datetime import datetime

Base = declarative_base()


class Sub(Base):
    # 表的名字:
    __tablename__ = 'SUB'

    def __init__(self):
        pass

    # 表的结构:

    link = Column(String, primary_key=True)
    title = Column(String)

    ar = Column(String)
    do_link = Column(String)
    do = Column(String)
    douban_link = Column(String)
    dtoken = Column(String)
    sid = Column(String)

    index = Column(String)
    season = Column(String)
    episode = Column(String)

    user = Column(String)
    team = Column(String)
    user_link = Column(String)
    team_link = Column(String)


    type = Column(String)
    lang = Column(String)
    bi_lang = Column(String)
    source = Column(String)
    format = Column(String)
    version = Column(String)

    info = Column(String)
    des = Column(String)

    attachement_link = Column(String)
    attachement_path = Column(String)
    attachement_status = Column(Integer)

    mod_date = Column(DateTime)
    status = Column(Integer)
    comment = Column(String)
    downloaded = Column(DateTime)

    def __str__(self) -> str:
        return "title: {}, link: {}".format(
            self.title, self.link)


class WorkItem(Base):
    # 表的名字:
    __tablename__ = 'WORK_ITEM'

    def __init__(self):
        pass

    # 表的结构:

    link = Column(String, primary_key=True)
    title = Column(String)
    title_other = Column(String)

    do = Column(String)
    douban_link = Column(String)

    info = Column(String)
    year = Column(String)
    area = Column(String)

    type = Column(String)
    director = Column(String)
    actor = Column(String)
    des = Column(String)

    mod_date = Column(DateTime)
    status = Column(Integer)
    comment = Column(String)
    downloaded = Column(DateTime)

    def __str__(self) -> str:
        return "title: {}, link: {}".format(
            self.title, self.link)