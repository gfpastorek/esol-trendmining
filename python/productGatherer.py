#!/usr/bin/python

import ebayAPI
import urllib
import re
from BeautifulSoup import BeautifulSoup


class ProductGatherer:
    def __init__(self, db):
        self.db = db


    def gather_ebayProducts(self, search_terms, category_list):
        ebayReader = ebayAPI.EbayAPI(self.db)
        for search_term in search_terms:
            print "Gathering products for search_term = " + search_term
            ebayReader.addProductsToDb(search_term, category_list)


    def gather_cnetProducts(self):
        pass


    def get_ebay_product_ids(self, category=None):
        return self.db.get_ebayProductIDs(category)


    def get_cnet_product_ids(self, category=None):
        return self.db.get_cnetProductIDs(category)


def removeTags(s):
    r = re.compile(r'<[^<>]*>')
    return r.sub('', s)


def getDoc(url):
    f = urllib.urlopen(url)
    doc = BeautifulSoup(f.read())
    f.close()
    return doc