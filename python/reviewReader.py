#!/usr/bin/python

import analyzer
import os
import sys

class ReviewReader:



	def __init__(self, db, pro_file, con_file, comp_file, retrain, retrain_comparative):
		self.db = db
		self.analyzer = analyzer.Analyzer()
		self.PRO_FILE = pro_file
		self.CON_FILE = con_file
		self.COMPARATIVE_FILE = comp_file
		self.RETRAIN = retrain
		self.RETRAIN_COMPARATIVE = retrain_comparative
		#self.CLASSIFIER_FILE = classifier_file
		#self.COMPARATIVE_CLASSIFIER_FILE = COMPARATIVE_CLASSIFIER_FILE
		#self.COMPARATIVE_TYPE_CLASSIFIER_FILE = COMPARATIVE_TYPE_CLASSIFIER_FILE



	def buildClassifier(self, classifier_file, comparative_classifier_file, comparative_type_classifier_file):

		if not self.analyzer.load_classifier(fname=classifier_file) or self.RETRAIN:
			'''Train on Pros'''
			dir = os.path.dirname(__file__)
			file_path = os.path.join(dir, self.PRO_FILE)
			with open(file_path, 'r') as f:
				text = f.readlines()
			self.analyzer.parse_training_data(text, 1)

			'''Train on Cons'''
			dir = os.path.dirname(__file__)
			file_path = os.path.join(dir, self.CON_FILE)
			with open(file_path, 'r') as f:
				text = f.readlines()
			self.analyzer.parse_training_data(text, -1)

			'''build classifier'''
			self.analyzer.train_NaiveBayes()

			self.analyzer.save_classifier(fname=classifier_file)


		if not self.analyzer.load_comparative_classifier(fname=comparative_classifier_file, fname2=comparative_type_classifier_file) or self.RETRAIN_COMPARATIVE:
			
			file_path = self.COMPARATIVE_FILE
			self.analyzer.parse_comparative_training_data(file_path)
			self.analyzer.train_comparative_NaiveBayes()
			self.analyzer.save_comparative_classifier(fname=comparative_classifier_file, fname2=comparative_type_classifier_file)


		
	def readAllNewReviews(self):
		pass


	def readNewReviews(self, product_id, start_date, end_date, new_only):
		
		if not self.analyzer.trained or not self.analyzer.comparative_trained:
			raise Exception("ERROR - Classifier is not trained")
			
		self.analyzer.clear_scores()

		num = 0

		self.analyzer.set_product(product_id)

		for review_data in _getNextNewReview(self.db, product_id, start_date, end_date, new_only):
			if review_data is None:
				continue
			date = review_data["date"]
			text = review_data["text"]
			quality = review_data["quality"]
			
			self.analyzer.analyze_review(text, quality)

			num += 1

		print "Read " + str(num) + " reviews"
		sys.stdout.flush()

		result_tuple = self.analyzer.dump_scores(num)

		freq_feats = result_tuple[0]
		scores = result_tuple[1]
		weighted_scores = result_tuple[2]

		for feat in freq_feats:
			feat_data_tuple = self.db.get_feat_data(product_id, feat[0], start_date, end_date)
			if feat_data_tuple is None:
				self.db.insert_feat(product_id, feat[0], feat[1], scores.get(feat[0], 0), weighted_scores.get(feat[0],0), start_date, end_date)
			else:
				freq = feat[1]+feat_data_tuple[0]
				if freq < 0:
					print "freq = " + str(freq)
				score = scores.get(feat[0],0)+feat_data_tuple[1]
				weighted_score = weighted_scores.get(feat[0],0)+feat_data_tuple[2]
				self.db.update_feat(product_id, feat[0], freq, score, weighted_score, start_date, end_date)

		self.analyzer.clear_scores()

		return True



"""
{source, review, date, poster, review_quality}
This is a generator
"""
def _getNextNewReview(db, product_id, start_date, end_date, new_only):

	new_review_ids = db.get_New_Review_ids(product_id, start_date, end_date, new_only)

	if new_review_ids is None:
		yield None

	for review_id in new_review_ids:
		review_data = dict()
		review_data_tuple = db.get_review_data(review_id[0])
		if review_data_tuple is None:
			continue
		review_data["source"] = review_data_tuple[0]
		review_data["text"] = review_data_tuple[1]
		review_data["date"] = review_data_tuple[2]
		review_data["poster"] = review_data_tuple[3]
		review_data["quality"] = review_data_tuple[4]
		db.set_review_notnew(review_id[0])
		yield review_data


def _date_between(date, start, end):
	return True

