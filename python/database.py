#!/usr/bin/python

import os
import sqlite3
import json
import sys

"""
SCHEMA:
cnet_review_url(INT cnet_product_id, INT product_id, TEXT name, TEXT cnet_url)
ebay_review_urls(INT ebay_product_id, INT product_id, TEXT name, TEXT ebay_url)
reviews(review_id, product_id, source_product_id, source, review, date, poster, review_quality, new)
features(feature_id, product_id, feature, frequency, score, start_date, end_date)

"""


class Database:
    def __init__(self, filename='db.sqlite'):
        dir = os.path.dirname(__file__)
        self.file_path = os.path.join(dir, filename)


    def connect(self):
        self.con = sqlite3.connect(self.file_path)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()


    def close(self):
        self.con.close()


    def addReviewRating(self, hash, rating):
        self.cur.execute("UPDATE reviews SET rating = ? WHERE hash = ?", (rating, hash))
        self.con.commit()


    def sourceState(self, source):
        try:
            self.cur.execute("SELECT state FROM sources WHERE name = ?", (source, ))
            result = self.cur.fetchone()
            if result == None:
                return None
            else:
                state = result[0]
                if state == None:
                    return None
        except:
            return None
        return json.loads(state)

    def setSourceState(self, source, state):
        state = json.dumps(state)
        self.cur.execute("UPDATE sources SET state = ? WHERE name = ?", (state, source))
        self.con.commit()

    '''
	cnet_review_urls(INT cnet_product_id, INT product_id, TEXT name, TEXT cnet_url)
	ebay_review_urls(INT ebay_product_id, INT product_id, TEXT name, TEXT ebay_url)
	reviews(review_id, product_id, source, review, date, poster, review_quality, new)
	features(feature_id, product_id, feature, frequency, score, start_date, end_date)
	'''

    def buildDatabase(self):

        if os.path.isfile(self.file_path):
            raise Exception("Error - Database Already Exists at " + self.file_path)

        else:
            self.con = sqlite3.connect(self.file_path)
            self.con.row_factory = sqlite3.Row
            self.cur = self.con.cursor()
            self.cur.execute("CREATE TABLE source(state TEXT, name TEXT)")
            self.cur.execute("CREATE TABLE status(last_ebay_date DATE, last_cnet_date DATE)")
            self.cur.execute(
                "CREATE TABLE products(product_id INTEGER PRIMARY KEY AUTOINCREMENT, family_line TEXT, model TEXT, brand TEXT, category TEXT, release_date DATE, cnet_title TEXT)")
            self.cur.execute(
                "CREATE TABLE ebay_products(ebay_product_id INT, product_id INT, ebay_url TEXT, review_count INT)")
            self.cur.execute(
                "CREATE TABLE cnet_products(cnet_product_id INT, product_id INT, cnet_url TEXT, review_count INT)")
            self.cur.execute(
                "CREATE TABLE reviews(review_id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INT, source_product_id INT, source TEXT, body TEXT, revdate DATE, poster TEXT, review_quality REAL, new INT, hash TEXT, pros TEXT, cons TEXT)")
            self.cur.execute(
                "CREATE TABLE features(feature_id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INT, feature TEXT, frequency INT, score INT, weighted_score INT, start_date DATE, end_date DATE)")
            self.cur.execute("CREATE TABLE ebay_spec(ebay_product_id INT, spec TEXT)")
            self.cur.execute("CREATE TABLE cnet_spec(cnet_product_id INT, spec TEXT)")
            self.cur.execute("CREATE TABLE feature_blacklist(feature TEXT, product_id INT)")
            self.con.commit()
            self.con.close()

    def fixblacklist(self):
        self.cur.execute("DROP TABLE feature_blacklist")
        self.con.commit()
        self.cur.execute("CREATE TABLE feature_blacklist(feature TEXT, product_id INT)")
        self.con.commit()


    def deleteDatabase(self):
        os.remove(self.file_path)
        return True


    def get_all_product_ids(self, new_only=False):
        if new_only:
            self.cur.execute("SELECT product_id FROM products", )
        else:
            self.cur.execute(
                "SELECT product_id FROM products WHERE 1 <= (SELECT 1 FROM reviews WHERE reviews.product_id=product_id AND new=1)", )
        return self.cur.fetchall()


    def get_earliest_review_date(self, product_id, new_only):
        if new_only:
            self.cur.execute("SELECT revdate FROM reviews WHERE product_id = ? AND new=1 ORDER BY revdate ASC",
                             (product_id,))
        else:
            self.cur.execute("SELECT revdate FROM reviews WHERE product_id = ? ORDER BY revdate ASC", (product_id,))
        result = self.cur.fetchone()
        if result is None:
            return None
        else:
            return result[0]


    def get_product_id(self, family_line, model, brand, cnet_title=None):
        if cnet_title is None:
            self.cur.execute("SELECT product_id FROM products WHERE family_line=? AND model=? AND brand=?",
                             (family_line, model, brand))
        else:
            self.cur.execute("SELECT product_id FROM products WHERE cnet_title=? AND brand=?", (cnet_title, brand))
        return self.cur.fetchone()


    def get_main_product_id(self, source_product_id, source):

        if source == "ebay":
            self.cur.execute("SELECT product_id FROM ebay_products WHERE ebay_product_id=?", (source_product_id,))
        elif source == "cnet":
            self.cur.execute("SELECT product_id FROM cnet_products WHERE cnet_product_id=?", (source_product_id,))
        else:
            raise Exception("ERROR - invalid source")

        self.con.commit()

        result = self.cur.fetchone()

        if result is None:
            return None
        else:
            return result[0]


    def add_product(self, family_line, model, brand, category, release_date):
        self.cur.execute("INSERT INTO products(family_line, model, brand, category, release_date) VALUES(?,?,?,?,?)",
                         (family_line, model, brand, category, release_date))
        self.con.commit()
        # need wait? else try fetching last product id +1
        self.cur.execute("SELECT product_id FROM products WHERE family_line=? AND model=? AND brand=?",
                         (family_line, model, brand))
        return self.cur.fetchone()


    def exists_ebayProduct(self, ebay_product_id):
        self.cur.execute("SELECT 1 FROM ebay_products WHERE ebay_product_id=?", (ebay_product_id,))
        return self.cur.fetchone() != None


    def add_ebayProduct(self, ebay_product_id, family_line, model, brand, ebay_url, category=None, release_date=None,
                        spec=None, review_count=None):
        result = self.get_product_id(family_line, model, brand)
        if result == None:
            result = self.add_product(family_line, model, brand, category, release_date)
        product_id = str(result[0])

        self.cur.execute(
            "INSERT INTO ebay_products(ebay_product_id, product_id, ebay_url, review_count) VALUES(?,?,?,?)",
            (ebay_product_id, product_id, ebay_url, review_count))
        self.con.commit()

        if spec != None:
            self.cur.execute("INSERT INTO ebay_spec(ebay_product_id, spec) VALUES(?,?)", (ebay_product_id, str(spec)))
            self.con.commit()


    def add_cnetProduct(self, cnet_product_id, manufacturer, title, cnet_url, category=None, release_date=None,
                        spec=None, review_count=None):
        split = title.split(" ", 1)
        family_line = split[0]
        model = split[1]
        result = self.get_product_id(family_line, model, manufacturer)
        if result == None:
            result = self.get_product_id(None, None, manufacturer, cnet_title=title)
            if result == None:
                result = self.add_product(None, None, manufacturer, category, release_date, cnet_title=title)
        product_id = str(result[0])

        self.cur.execute(
            "INSERT INTO cnet_products(cnet_product_id, product_id, cnet_url, review_count) VALUES(?,?,?,?)",
            (cnet_product_id, product_id, cnet_url, review_count))
        self.con.commit()

        if spec != None:
            self.cur.execute("INSERT INTO cnet_spec(cnet_product_id, spec) VALUES(?,?)", (cnet_product_id, str(spec)))
            self.con.commit()


    def get_ebayProductIDs(self, category=None):
        if category is None:
            self.cur.execute(
                "SELECT ebay_product_id FROM ebay_products INNER JOIN products ON ebay_products.product_id = products.product_id", )
        else:
            self.cur.execute(
                "SELECT ebay_product_id FROM ebay_products INNER JOIN products ON ebay_products.product_id = products.product_id WHERE category=?",
                (category,))
        result = self.cur.fetchall()
        return result

    def get_cnetProductIDs(self, category=None):
        if category is None:
            self.cur.execute(
                "SELECT cnet_product_id FROM cnet_products INNER JOIN products ON ebay_products.product_id = products.product_id", )
        else:
            self.cur.execute(
                "SELECT cnet_product_id FROM cnet_products INNER JOIN products ON ebay_products.product_id = products.product_id WHERE category=?",
                (category,))
        result = self.cur.fetchall()
        return result

    def get_eBayReviewPageURL(self, ebay_product_id):
        self.cur.execute("SELECT ebay_url FROM ebay_products WHERE ebay_product_id=?", (ebay_product_id,))
        return str(self.cur.fetchone()[0])

    def get_cnetReviewPageURL(self, source_product_id):
        self.cur.execute("SELECT cnet_url FROM cnet_products WHERE cnet_product_id=?", (cnet_product_id,))
        return str(self.cur.fetchone()[0])


    def has_reviews(self, source_product_id):
        self.cur.execute("SELECT 1 FROM reviews WHERE source_product_id=?", (source_product_id,))
        return self.cur.fetchone() != None

    def reviewExists(self, product_id, source_product_id, source, hash):
        self.cur.execute("SELECT 1 FROM reviews WHERE product_id=? AND source_product_id=? AND source=? AND hash=?",
                         (product_id, source_product_id, source, hash))
        return self.cur.fetchone() != None

    def addReview(self, product_id, source_product_id, source, body, date, poster, review_quality, hash, pros="",
                  cons=""):
        self.cur.execute(
            "INSERT INTO reviews(product_id, source_product_id, source, body, pros, cons, revdate, poster, review_quality, hash, new) VALUES (?,?,?,?,?,?,?,?,?, ?, 1)",
            (product_id, source_product_id, source, body, pros, cons, date, poster, review_quality, hash))
        self.con.commit()

    def updateReview(self, review_id, body, date, review_quality, hash, pros="", cons=""):
        self.cur.execute(
            "UPDATE reviews SET body=?, pros=?, cons=?, revdate=?, review_quality=?, hash=? WHERE review_id=?",
            (body, pros, cons, date, review_quality, hash, review_id))
        self.con.commit()

    def prevReviewExists(self, product_id, source_product_id, source, poster):
        self.cur.execute(
            "SELECT review_id FROM reviews WHERE product_id=? AND source_product_id=? AND source=? AND poster=?",
            (product_id, source_product_id, source, poster))
        return self.cur.fetchone()

    """date format: YYYY-MM-DD"""

    def get_New_Review_ids(self, product_id, start_date, end_date, new_only):
        if new_only:
            self.cur.execute(
                "SELECT review_id FROM reviews WHERE new = 1 AND product_id = ? AND (revdate BETWEEN ? AND ?)",
                (product_id, start_date, end_date))
        else:
            self.cur.execute("SELECT review_id FROM reviews WHERE product_id = ? AND (revdate BETWEEN ? AND ?)",
                             (product_id, start_date, end_date))
        return self.cur.fetchall()

    def get_review_data(self, review_id):
        self.cur.execute("SELECT source, body, revdate, poster, review_quality FROM reviews WHERE review_id = ?",
                         (review_id,))
        return self.cur.fetchone()

    def set_review_notnew(self, review_id):
        self.cur.execute("UPDATE reviews SET new = 0 WHERE review_id = ?", (review_id,))
        self.con.commit()

    def get_feat_data(self, product_id, feat, start_date, end_date):
        self.cur.execute(
            "SELECT frequency, score, weighted_score FROM features WHERE product_id=? AND feature=? AND start_date=? AND end_date=?",
            (product_id, feat, start_date, end_date))
        return self.cur.fetchone()

    def insert_feat(self, product_id, feat, freq, score, weighted_score, start_date, end_date):
        self.cur.execute(
            "INSERT INTO features(product_id, feature, frequency, score, weighted_score, start_date, end_date) VALUES (?,?,?,?,?,?,?)",
            (product_id, feat, freq, score, weighted_score, start_date, end_date))
        self.con.commit()

    def update_feat(self, product_id, feat, freq, score, weighted_score, start_date, end_date):
        self.cur.execute(
            "UPDATE features SET frequency=?, score=?, weighted_score=? WHERE product_id=? AND feature=? AND start_date=? AND end_date=?",
            (freq, score, weighted_score, product_id, feat, start_date, end_date))
        self.con.commit()


    def dump_query(self, query):
        for row in self.cur.execute(query):
            print [ele for ele in row]


    def dump_products(self):
        query = "SELECT * FROM products"
        for row in self.cur.execute(query):
            print [ele for ele in row]


    def dump_reviews(self):
        query = "SELECT * FROM reviews"
        rev_id = 0
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            print [ele for ele in row]


    def dump_features(self):
        query = "SELECT * FROM features"
        for row in self.cur.execute(query):
            print [ele for ele in row]

    def dump_series(self):
        query = "SELECT score, weighted_score, frequency, start_date FROM features WHERE product_id=18 AND feature='it' ORDER BY start_date"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            self.cur.execute(
                "SELECT SUM(frequency) as wordcount FROM features WHERE product_id=18 AND start_date = ? GROUP BY start_date",
                (row[3], ))
            wc = float(self.cur.fetchone()[0])
            print (float(row[0]) / wc, float(row[1]) / wc, float(row[2]) / wc, row[3], wc)
        print "\n---------------\n"
        query = "SELECT score, weighted_score, frequency, start_date FROM features WHERE product_id=18 AND feature='it' ORDER BY start_date"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            self.cur.execute(
                "SELECT SUM(frequency) as wordcount FROM features WHERE product_id=18 AND start_date = ? GROUP BY start_date",
                (row[3], ))
            wc = float(1)
            print (float(row[0]) / wc, float(row[1]) / wc, float(row[2]) / wc, row[3])


    def delete_features(self):
        query = "DELETE FROM features"
        self.cur.execute(query)
        self.con.commit()

    def delete_reviews(self):
        query = "DELETE FROM reviews"
        self.cur.execute(query)
        self.con.commit()

    def fix_dates(self):
        query = "SELECT DISTINCT revdate FROM reviews"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            old_date = str(row[0])
            split = old_date.split("-")
            if len(split) < 2:
                print len(split), split
            if len(split[1]) < 2:
                split[1] = "0" + split[1]
            if len(split[2]) < 2:
                split[2] = "0" + split[2]
            new_date = split[0] + "-" + split[1] + "-" + split[2]
            self.cur.execute("UPDATE reviews SET revdate = ? WHERE revdate = ?", (new_date, old_date))
            self.con.commit()

        query = "SELECT DISTINCT start_date, end_date FROM features"
        self.cur.execute(query)
        rows = self.cur.fetchall()
        for row in rows:
            old_start = str(row[0])
            old_end = str(row[1])

            # split = old_start.split("-")
            #if len(split) < 2:
            #	print len(split), split
            #	continue
            #if len(split[1]) < 2:
            #	split[1] = "0" + split[1]
            #if len(split[2]) < 2:
            #	split[2] = "0" + split[2]
            #new_start = split[0] + "-" + split[1] + "-" + split[2]
            old_start = 0

            split = old_end.split("-")
            if len(split[1]) < 2:
                split[1] = "0" + split[1]
            if len(split[2]) < 2:
                split[2] = "0" + split[2]
            new_end = split[0] + "-" + split[1] + "-" + split[2]

            if (int(split[1]) == 1):
                split[0] = str(int(split[0]) - 1)
                split[1] = "12"
            else:
                split[1] = str(int(split[1]) - 1)
                if len(split[1]) < 2:
                    split[1] = "0" + split[1]

            new_start = split[0] + "-" + split[1] + "-" + split[2]

            #query = "UPDATE features SET start_date = \'" + str(new_start) +  "\' AND end_date = \'" + str(new_end) + "\' WHERE end_date = \'" + str(old_end) + "\'"
            #print query
            #sys.stdout.flush()

            #self.cur.execute(query)
            self.cur.execute("UPDATE features SET start_date = ?, end_date = ? WHERE end_date = ?",
                             (new_start, new_end, old_end))
            self.con.commit()