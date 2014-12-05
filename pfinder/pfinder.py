__author__ = 'pzc'

from bs4 import BeautifulSoup
from skylark import Model, Field, PrimaryKey, Database
from urls import Url, UrlPatterns
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
            'http://www.kuaidaili.com/free/outha/',
            'http://www.kuaidaili.com/free/outtr/',
            'http://www.xici.net.co/nn/',
            'http://www.xici.net.co/nt/',
            'http://www.xici.net.co/wn/',
            'http://www.xici.net.co/wt/',
        ]
        if dbname != 'proxy.db':
            self._init_db(dbname)
        else:
            self._init_db(os.path.join(os.path.split(os.path.realpath(__file__))[0], dbname))

        self.url_patterns = UrlPatterns(
            Url(r'http://www.kuaidaili.com/free.*', 'kuaidaili', self._parse_kuaidaili),
            Url(r'http://www.xici.net.co/.*', 'xici', self._parse_xici),
        )

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

    def _parse(self, url, force=False):
        parser = self.url_patterns.get_parser(url)
        html = self._get_html(url)
        parser(html, force=force)

    def _parse_xici(self, html, force=False):
        bs = BeautifulSoup(html)
        table = bs.find('table', attrs={'id': 'ip_list'})
        for tr in table.find_all('tr')[1:]:
            tds = tr.find_all('td')
            ip = tds[2].text.strip()
            port = tds[3].text.strip()
            anonymous = tds[5].text.strip()
            proxy_type = tds[6].text.strip()
            location = tds[4].text.strip()
            response_time = tds[8].text.strip()
            last_confirm = tds[9].text.strip()
            ip_port = ':'.join([ip, port])

            if Proxy.findone(ip_port=ip_port):
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

    def _parse_kuaidaili(self, html, force=False):
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

            if Proxy.findone(ip_port=ip_port):
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
                self._parse(url + '%s/' % page, force=force)
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

    def clean_dead_proxy(self):
        dead_proxies = Proxy.findall(is_dead=True)
        if dead_proxies:
            for proxy in dead_proxies:
                proxy.destroy()
        return len(dead_proxies)

    def count_proxy(self, filter_dead=False):
        if filter_dead:
            return len(Proxy.findall(is_dead=False))
        else:
            return len(Proxy.findall())


if __name__ == '__main__':
    # while True:
    #     for url in urls:
    #         getProxy(url)
    #     break
    #     time.sleep(60*30)
    pfinder = ProxyFinder()
    ip_port = pfinder.get_proxy(force=True)
    # # pfinder.mark_proxy(ip_port)
    # # pfinder.clean_dead_proxy()
    # print ip_port


    # print res

