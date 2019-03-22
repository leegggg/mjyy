import datetime

scoreUrlFormat = "https://www.meijutt.com/inc/ajax.asp?id={}&action=newstarscorevideo"
forumPageFormat = 'http://www.btbtt06.com/forum-index-fid-{}-page-{}.htm'
threadRegexp = 'http://www.btbtt06.com/thread-index-fid-(?P<fid>[0-9]+)-tid-(?P<tid>[0-9]+).htm'
attachementRegexp = 'http://www.btbtt06.com/attach-dialog-fid-(?P<fid>[0-9]+)-aid-(?P<aid>[0-9]+)-ajax-1.htm'
attachementUrlFormat = "http://www.btbtt06.com/attach-download-fid-{}-aid-{}.htm"
DB_URL = 'sqlite:///./data/btbtt06.com.db'
FID_IMG = 'IMG'
FALL_BACKDATE = datetime.datetime(1970, 1, 1, 0, 0, 0)
REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
}

NB_MAX_BLOCKED = 10
FATCH_SIZE = 200
