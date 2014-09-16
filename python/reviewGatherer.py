#!/usr/bin/python

from BeautifulSoup import BeautifulSoup, SoupStrainer
import urllib
import analyzer
import time
from time import strptime, mktime
import re
import hashlib
import sys

"""
TODO - determine if there are new reviews
"""

'''
eBay API keys:
Name: Key Set 1 Edit Name
DEVID: 3e02119d-485c-4f60-9064-92082e01e8dc
AppID: GregoryF-64c3-45f0-85eb-302a78a558a9
CertID: d4b2501e-cfe1-432f-a6f3-0a6fea562f3e
'''


class ReviewGatherer:
    def __init__(self, db, source):
        self.db = db
        self.source = source


    def getReviewPageURL(self, page, source_product_id=-1, url=None):

        if self.source == 'ebay':
            if url is None:
                url = self.db.get_eBayReviewPageURL(source_product_id)
            return str(url) + "&_pgn=" + str(page)

        elif self.source == 'cnet':
            if url is None:
                url = self.db.get_cnetReviewPageURL(source_product_id)
            return str(url)[:-5] + "-" + str(page) + ".html"


    def getNumPages(self, url):
        try:
            if self.source == 'ebay':
                doc = getDoc(url)
                span = str(doc.find('span', 'pgn-gt-sp'))
                span = span.split("of <span>")[1]
                result = span.split("</span>")[0]
                return int(result)
            elif self.source == 'cnet':
                pass
        except:
            print "Exception for getNumPages on " + url
            return 1


    def getCorrectUrl(self, url):
        try:
            if self.source == 'ebay':
                doc = getDoc(url)
                span = doc.find('span', 'pgn-n')
                url = str(span.find('a')['href'])
                return "http://www.ebay.com" + url
            elif self.source == 'cnet':
                return url
        except:
            print "Exception for getCorrectUrl on " + url
            return None


    def crawl_ebayReviewPage(self, url, product_id, source_product_id):

        strainer = SoupStrainer('div', {'class': 'rvp-w'})

        reviews = getDoc(url, strainer)

        count = 0

        # reviews = doc.findAll('div', 'rvp-w')

        for review in reviews:

            body = review.find('div', attrs={'itemprop': 'reviewBody'})
            body = body.find('p')
            if body is None:
                continue
            body = str(body).replace("<br />", "\n")
            body = removeTags(body.replace("<br />", "\n"))
            body = body.decode("utf-8")

            poster = review.find('div', 'ds3mb')
            poster = removeTags(str(poster.find('a')))
            poster = poster.decode("utf-8")

            date = review.find('span', 'rvp-cd rvp-r')
            date = removeTags(str(date))
            date = strptime(date[9:], '%m/%d/%y')
            date = str(date.tm_year) + "-" + str(date.tm_mon) + "-" + str(01)
            date = self.fix_date_string(date)

            helpful = review.find('span', 'rvp-rhw')
            helpful = helpful.findAll('strong')
            if len(helpful) < 1:
                helpful = float(0.5)
            else:
                helpful_true = removeTags(str(helpful[0]))
                helpful_false = removeTags(str(helpful[1]))
                helpful = float(helpful_true) / (float(helpful_true) + float(helpful_false))

            hash = str(_hashReview(body, helpful)).encode("utf-8")
            #if not self.db.reviewExists(product_id, source_product_id, 'ebay', hash):
            if True:
                result_tuple = self.db.prevReviewExists(product_id, source_product_id, 'ebay', poster)
                if result_tuple is None:
                    self.db.addReview(product_id, source_product_id, 'ebay', body, date, poster, helpful, hash)
                    print "Added review"
                else:
                    #self.db.updateReview(result_tuple[0], body, date, helpful, hash)
                    print "Existing review"
                count += 1

        print "Add " + str(count) + " reviews"


    def fix_date_string(self, old_date):
        split = old_date.split("-")
        if len(split[1]) < 2:
            split[1] = "0" + split[1]
        if len(split[2]) < 2:
            split[2] = "0" + split[2]
        return split[0] + "-" + split[1] + "-" + split[2]


def _hashReview(body, helpful):
    m = hashlib.md5()
    try:
        m.update(body)
    except:
        m.update(body.encode('utf8', 'ignore'))
    try:
        m.update(str(helpful))
    except:
        m.update(body.encode('utf8', 'ignore'))
    return m.hexdigest()


def removeTags(s):
    r = re.compile(r'<[^<>]*>')
    return r.sub('', s)


def getDoc(url, strain=None):
    count = 0
    while True:
        try:
            f = urllib.urlopen(url)
            if strain is None:
                doc = BeautifulSoup(f.read())
            else:
                doc = BeautifulSoup(f.read(), parseOnlyThese=strain)
            f.close()
            return doc
        except Exception as e:
            print e
            count += 1
            if count > 100:
                raise Exception(e)