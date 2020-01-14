import datetime

scoreUrlFormat = "https://www.meijutt.com/inc/ajax.asp?id={}&action=newstarscorevideo"
forumPageFormat = 'http://www.btbtt06.com/forum-index-fid-{}-page-{}.htm'
threadRegexp = 'http://www.btbtt06.com/thread-index-fid-(?P<fid>[0-9]+)-tid-(?P<tid>[0-9]+).htm'
attachementRegexp = 'http://www.btbtt06.com/attach-dialog-fid-(?P<fid>[0-9]+)-aid-(?P<aid>[0-9]+)-ajax-1.htm'
attachementUrlFormat = "http://www.btbtt06.com/attach-download-fid-{}-aid-{}.htm"
attachementUrlRegexp = "http://www.btbtt06.com/attach-download-fid-(?P<fid>[0-9]+)-aid-(?P<aid>[0-9]+).htm"

FEED_URL = 'https://subhd.tv/feed'
DOWN_URL = 'https://subhd.tv/ajax/down_ajax'

ATTACHEMENT_BASE_PATH = 'e:\subhd\server'
ATTACHEMENT_OUT_BASE_PATH = 'e:\subhd\out'

# ATTACHEMENT_BASE_PATH = './data/subhd/server'
# ATTACHEMENT_OUT_BASE_PATH = './data/subhd/out'

DOMAIN = 'https://subhd.tv'

REGEXP_INDEX = r'S(?P<s>[0-9]+)EP?(?P<e>[0-9\-]+)'

REGEXP_DOUBAN = r'https?://movie.douban.com/subject/[0-9]+'
REGEXP_AR = r'/ar0/(?P<id>[0-9]+)'
REGEXP_AR_FULL = r'https?://subhd.tv(?P<path>/ar0/(?P<id>[0-9]+))'
REGEXP_ZU = r'/zu/(?P<id>[0-9]+)'
REGEXP_USER = r'/u/(?P<id>.+)'
REGEXP_DO = r'/do0/(?P<id>.+)'
REGEXP_URL_FULL = r'https?://[^/]+(?P<path>/.+)'

REGEXP_TYPE = r'类别：(?P<text>.+)'
REGEXP_LANG = r'语言：(?P<text>.+)'
REGEXP_BI_LANG = r'双语：(?P<text>.+)'
REGEXP_SOURCE = r'来源：(?P<text>.+)'
REGEXP_FORMAT = r'格式：(?P<text>.+)'
REGEXP_VERSION = r'字幕版本：(?P<text>.+)'

REGEXP_YEAR_AREA = r'(?P<year>[0-9]+)年[\s+](?P<area>[^\s]+)'
REGEXP_ITEM_TYPE = r'类型：(?P<text>.+)'
REGEXP_ITEM_DIRECTOR = r'导演：(?P<text>.+)'
REGEXP_ITEM_ACTOR = r'演员：(?P<text>.+)'
REGEXP_ITEM_DES = r'介绍：(?P<text>.+)'

DB_URL = 'sqlite:///./data/subhd.com.db'
FID_IMG = 'IMG'
FALL_BACKDATE = datetime.datetime(1970, 1, 1, 0, 0, 0)
REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
}

NB_MAX_BLOCKED = 10
FATCH_SIZE = 200

STATUS_UNKNOW_ERROR = 1000
STATUS_SUB_FORBIDDEN_ERROR = 2403