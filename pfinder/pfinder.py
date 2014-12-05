__author__ = 'pzc'

from bs4 import BeautifulSoup
from skylark import Model, Field, PrimaryKey, Database
import urllib2
import gzip
import StringIO
import time
import sqlite3
import os.path


class AlreadyExistError(Exception): pass


class Proxy(Model):
    #from kuaidaili.com
    ip_port = PrimaryKey()
    anonymous = Field()
    proxy_type = Field()
    location = Field()
    response_time = Field()
    last_confirm = Field()

    #for later use
    last_used = Field()
    is_dead = Field()


class ProxyFinder():
    def __init__(self, dbname='proxy.db'):
        self.urls = [
            'http://www.kuaidaili.com/free/inha/',
            'http://www.kuaidaili.com/free/intr/',
        ]
        if dbname != 'proxy.db':
            self._init_db(dbname)
        else:
            self._init_db(os.path.join(os.path.split(os.path.realpath(__file__))[0], dbname))

    def _init_db(self, dbname):
        if not os.path.exists(dbname):
            cx = sqlite3.connect(dbname)
            cursor = cx.cursor()
            res = cursor.execute("""
                create table proxy (
                    ip_port varchar primary key,
                    anonymous varchar,
                    proxy_type varchar,
                    location varchar,
                    response_time varchar,
                    last_confirm varchar,
                    last_used float,
                    is_dead boolean
                  )
              """)
            print 'create a new proxy database in: %s' % os.path.abspath(dbname)
        Database.set_dbapi(sqlite3)
        Database.config(db=dbname)

    def _parse(self, html, force=False):
        bs = BeautifulSoup(html)
        tbody = bs.find('tbody')
        for tr in tbody.find_all('tr'):
            tds = tr.find_all('td')
            ip = tds[0].text.strip()
            port = tds[1].text.strip()
            anonymous = tds[2].text.strip()
            proxy_type = tds[3].text.strip()
            location = tds[4].text.strip()
            response_time = tds[5].text.strip()
            last_confirm = tds[6].text.strip()
            ip_port = ':'.join([ip, port])

            if Proxy.findone(ip_port=ip_port, is_dead=False):
                if not force:
                    raise AlreadyExistError
            else:
                Proxy.create(
                    ip_port=ip_port,
                    anonymous=anonymous,
                    proxy_type=proxy_type,
                    location=location,
                    response_time=response_time,
                    last_confirm=last_confirm,
                    last_used=0.0,
                    is_dead=0
                )

    def _get_html(self, url):
        response = urllib2.urlopen(url)
        if response.info().get('Content-Encoding') == 'gzip':
            buf = StringIO.StringIO(response.read())
            gzip_f = gzip.GzipFile(fileobj=buf)
            html = gzip_f.read()
        else:
            html = response.read()
        encoding = response.headers['content-type'].split('charset=')[-1]
        return unicode(html, encoding)

    def _add_proxy(self, url, pages=5, force=False):
        try:
            for page in xrange(1, pages+1):
                html = self._get_html(url+'%s/'%page)
                self._parse(html, force=force)
                time.sleep(1)
        except AlreadyExistError:
            # print 'already exist!'
            pass

    def delete_proxy(self, ip_port):
        proxy = Proxy.findone(ip_port=ip_port)
        if proxy:
            proxy.destroy()

    def mark_proxy(self, ip_port, is_dead=False):
        proxy = Proxy.findone(ip_port=ip_port)
        if proxy:
            if is_dead:
                proxy.is_dead = 1
            proxy.last_used = time.time()
            proxy.save()

    def get_proxy(self, beyond_seconds=None, force=False):
        if force:
            for url in self.urls:
                self._add_proxy(url, force=force)

        proxy = Proxy.findone(last_used=0.0, is_dead=False)
        if not proxy and beyond_seconds:
            timestamp = time.time() - beyond_seconds
            proxy = Proxy.findone(Proxy.last_used < timestamp, is_dead=False)
        if not proxy:
            for url in self.urls:
                self._add_proxy(url, force=force)
                proxy = Proxy.findone(last_used=0.0, is_dead=False)
        if not proxy:
            return None

        return proxy.ip_port



if __name__ == '__main__':
    # while True:
    #     for url in urls:
    #         getProxy(url)
    #     break
    #     time.sleep(60*30)
    pfinder = ProxyFinder()
    ip_port = pfinder.get_proxy()
    # pfinder.mark_proxy(ip_port)
    print ip_port

    # print res

