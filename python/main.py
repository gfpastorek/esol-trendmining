#!/usr/bin/python

import database
import reviewGatherer
import reviewReader
import productGatherer
import datetime
import sys


'''
args

GATHER_PRODUCTS, default True
GATHER_REVIEWS, default True
READ_REVIEWS, default True
SKIP_OLD_REVIEWS, default True
CLEAR_DATABASE, default False

command line example:
python main.py -CLEAR_DATABASE=True -SKIP_OLD_REVIEWS=False

'''

RETRAIN = False
CLASSIFIER_FILE = 'Classifiers\\review_classifier.pickle'
COMPARATIVE_CLASSIFIER_FILE = 'Classifiers\\comparative_classifier.pickle'
COMPARATIVE_TYPE_CLASSIFIER_FILE = 'Classifiers\\comparative_type_classifier.pickle'

RETRAIN_COMPARATIVE = False
PRO_FILE = "TrainingData\\IntegratedPros.txt"
CON_FILE = "TrainingData\\IntegratedCons.txt"
COMPARATIVE_FILE = "TrainingData\\labeledSentences.txt"

DB_FILE = 'db.sqlite'

ebay_domain_list = ['Cell Phones']

ebay_keyword_list = [
                     'htc', 'iphone', 'sony', 'samsung', 'droid', 'blackberry', 'motorola',
					 'nokia', 'huawei', 'zte', 'android', 'lg', 'rm', 'pantech', 'hp', 'dell',
					 'doro', 'boost', 'casio', 'palm', 'peek', 'rim', 't-mobile', 'sanyo',
					 'verizon', 'tracfone', 'utstarcom', 'sonim', 'iphone 5s'
]

args = {}

args['GATHER_PRODUCTS'] = True
args['GATHER_REVIEWS'] = True
args['READ_REVIEWS'] = True
args['SKIP_OLD_REVIEWS'] = True
args['CLEAR_DATABASE'] = False


def getArgs():
    global args
    for arg in sys.argv:
        if arg[0] != '-':  # check valid arg
            continue
        arg_split = arg.split('=')  # check valid arg
        if len(arg_split) < 2:
            continue
        param = str(arg_split[0][1:])
        value = bool(arg_split[1])
        args[param] = value
    return args


def gatherEbayReviews(db, ebayReviewGatherer, product_gatherer):
    for row in product_gatherer.get_ebay_product_ids():
        ebay_product_id = row[0]
        product_id = db.get_main_product_id(ebay_product_id, 'ebay')
        url = str(ebayReviewGatherer.getReviewPageURL(1, source_product_id=ebay_product_id))
        print "Gathering reviews at url = " + url
        num_pages = ebayReviewGatherer.getNumPages(url)
        url = url.split('&_pgn')[0]
        url = ebayReviewGatherer.getCorrectUrl(url)

        for i in range(1, num_pages + 1):
            print "On page - " + str(i) + "of " + str(num_pages)
            if url is None:
                continue
            if len(url.split('&_pgn')) > 0:
                url = url.split('&_pgn')[0]
            url = str(ebayReviewGatherer.getReviewPageURL(i, url=url))
            ebayReviewGatherer.crawl_ebayReviewPage(url, product_id, ebay_product_id)


def readReviewsForProduct(args, current_date, earliest_date, product_id, reader):
    earliest_date = datetime.datetime.strptime(earliest_date, "%Y-%m-%d").date()
    day_count = (current_date - earliest_date).days + 1
    for _date in (earliest_date + datetime.timedelta(n) for n in xrange(0, day_count, 32)):
        start_date = _date.strftime("%Y-%m-%d")
        split = start_date.split("-")
        start_date = split[0] + "-" + split[1] + "-01"

        _date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()

        end_date = (_date + datetime.timedelta(days=32)).strftime("%Y-%m-%d")
        split = end_date.split("-")
        end_date = split[0] + "-" + split[1] + "-01"

        print "Reviews between " + str(start_date) + " and " + str(end_date)

        reader.readNewReviews(product_id, start_date, end_date, args['SKIP_OLD_REVIEWS'])


def readReviews(args, db, reader):
    print "Building the classifiers"
    reader.buildClassifier(CLASSIFIER_FILE, COMPARATIVE_CLASSIFIER_FILE, COMPARATIVE_TYPE_CLASSIFIER_FILE)
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    split = current_date.split("-")
    current_date = split[0] + "-" + split[1] + "-" + "1"
    current_date = datetime.datetime.strptime(current_date, "%Y-%m-%d").date()
    for row in db.get_all_product_ids(new_only=False):
        product_id = row[0]
        print "Reading reviews for product " + str(product_id)
        earliest_date = db.get_earliest_review_date(product_id, args['SKIP_OLD_REVIEWS'])
        if not earliest_date is None:
            readReviewsForProduct(args, current_date, earliest_date, product_id, reader)


def main():

    '''parse args'''
    args = getArgs()

    db = database.Database(DB_FILE)

    if args['CLEAR_DATABASE']:
        db.deleteDatabase()
        db.buildDatabase()

    db.connect()

    product_gatherer = productGatherer.ProductGatherer(db)
    ebayReviewGatherer = reviewGatherer.ReviewGatherer(db, 'ebay')
    reader = reviewReader.ReviewReader(db, PRO_FILE, CON_FILE, COMPARATIVE_FILE, RETRAIN, RETRAIN_COMPARATIVE)

    '''GATHER EBAY REVIEWS'''
    print "Gathering Ebay Products"

    if args['GATHER_PRODUCTS']:
        product_gatherer.gather_ebayProducts(ebay_keyword_list, ebay_domain_list)
        print "Found all products"

    if args['GATHER_REVIEWS']:
        gatherEbayReviews(db, ebayReviewGatherer, product_gatherer)
        print "Got all reviews"

    '''ANALYZE REVIEWS'''
    if args['READ_REVIEWS']:
        readReviews(args, db, reader)

    db.close()


def _url_invalid(url):
    return False


if __name__ == '__main__':
    main()