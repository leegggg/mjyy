
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

def getAllInfo(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    results = session.query(Page).filter(Page.link == 'https://www.meijutt.com/content/meiju1315.html').first()
    info = results.info
    soup: BeautifulSoup = BeautifulSoup(results.info, 'html.parser')
    lis:[Tag] = soup.select('em')
    pass

def main():
    engine = create_engine('sqlite:///./data/meijutt.com.db')
    Base.metadata.create_all(engine)
    getAllInfo(engine)
    # getNews(engine)


    return


if __name__ == '__main__':
    main()


    "#message_27697"