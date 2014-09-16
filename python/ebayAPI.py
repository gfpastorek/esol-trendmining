#!/usr/bin/python

import urllib
from ebaysdk.shopping import Connection as shopping
import database
import sys
from BeautifulSoup import BeautifulSoup
import re

'''
SANDBOX eBay API keys:
Name: Key Set 1 Edit Name
DEVID: 3e02119d-485c-4f60-9064-92082e01e8dc
AppID: GregoryF-64c3-45f0-85eb-302a78a558a9
CertID: d4b2501e-cfe1-432f-a6f3-0a6fea562f3e


PRODUCTION KEYS
Name:
Key Set 1 Edit Name
DEVID:
3e02119d-485c-4f60-9064-92082e01e8dc
AppID:
GregoryF-2cbe-4646-90fd-cb2c8ee459f4
CertID:
7a474719-992e-42a7-b73d-ea1dc4a38c58

'''

APP_ID = "GregoryF-2cbe-4646-90fd-cb2c8ee459f4"
DEV_ID = "3e02119d-485c-4f60-9064-92082e01e8dc"
CERT_ID = "7a474719-992e-42a7-b73d-ea1dc4a38c58"

attr_list = [
    "Brand", "Family Line", "Model", "Storage Capacity", "Band", "Camera",
    "Battery Capacity", "Touch Screen", "Bluetooth", "Digital Camera", "GPS",
    "Email Access", "Internet Browser", "Speakerphone"
]

categories = {
    'Cell Phones & Smartphones': 9355
}


class EbayAPI:
    def __init__(self, db):
        self.api = shopping(appid=APP_ID)
        self.db = db


    def addProductsToDb(self, search_term, domain_list):

        try:
            self.api.execute('FindProducts', {'QueryKeywords': str(search_term), 'MaxEntries': str(100)})

            product_list = self.api.response_dict().Product

            for product in list(product_list):

                domain = product.DomainName
                if not domain in domain_list:
                    continue

                productId = product.ProductID.value.encode("utf-8")
                review_url = "http://www.ebay.com/rvw/Samsung-A-A/" + str(
                    productId) + "?rt=nc&_dmpt=Cell_Phones&_pcategid=9355&_pcatid=801&_trksid=p5797.c0.m1724&_pgn=1"
                detailsURL = product.DetailsURL.encode("utf-8")
                review_count = product.ReviewCount

                sys.stdout.write("Found product = " + str(productId) + "\n")
                sys.stdout.flush()

                if not self.db.exists_ebayProduct(productId):
                    spec = _getSpecData(detailsURL)
                    self.db.add_ebayProduct(productId, spec.get("Family Line", ""), spec.get("Model", ""),
                                            spec.get("Brand", ""), review_url, category=domain, spec=spec,
                                            review_count=review_count)
                    print "Added " + str(spec.get("Model"))
        except Exception as e:
            print "Ebay API error for search_term = " + search_term
            print e


def _getSpecData(url):
    doc = _getDoc(url)
    tr_list = doc.findAll('tr')

    spec = {}

    for tr in tr_list:
        td_list = tr.findAll('td')
        if len(td_list) != 2:
            continue
        td1 = _removeTags(str(td_list[0])).encode("utf-8")
        td2 = _removeTags(str(td_list[1])).encode("utf-8")
        for attr in attr_list:
            if td1 == attr:
                spec[attr] = td2
                break

    return spec


def _removeTags(s):
    if s == "" or s is None:
        return ""
    r = re.compile(r'<[^<>]*>')
    return r.sub('', s)


def _getDoc(url):
    f = urllib.urlopen(url)
    doc = BeautifulSoup(f.read())
    f.close()
    return doc
		 