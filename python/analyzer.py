#!/usr/bin/python


import string
import nltk
import nltk.classify.util
import nltk.data
import multiprocessing
import re
import operator
from BeautifulSoup import BeautifulSoup
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import wordnet
import badwords
import lexicons
import pickle
import os.path
import os
import copy

'''_'''
COMPACTNESS_THRESHOLD = 2

'''frequency threshold per review'''
FREQUENCY_THRESHOLD = 0.1

'''_'''
OPINION_WORD_DIST_THRESHOLD = 8

'''_'''
SHIFTER_DIST_THRESHOLD = 4

'''_'''
OMIT_PUNCTUATION = False

'''_'''
COMPARATIVE_DIST_THRESHOLD = 6


class Analyzer:


	def __init__(self):
		self.trained = False
		self.comparative_trained = False
		self.comparative_type_trained = False
		self.training_data = []
		self.comparative_training_data = []
		self.comparative_type_training_data = []
		self.feats = []
		self.scores = []
		self.prune_infreq = True
		self.product = ""
		self.feats = {}
		self.scores = {}
		self.weighted_scores = {}
		self.review_count = 0

	def save_classifier(self, fname):
		if self.trained:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			f = open(fpath, 'wb')
			pickle.dump(self.classifier, f)
			f.close()
			return True
		else:
			print "ERROR - NOT TRAINED"
			return False


	def load_classifier(self, fname):
		try:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			if os.path.isfile(fpath):
				f = open(fpath)
				self.classifier = pickle.load(f)
				f.close()
				self.trained = True
				print "Classifier Loaded"
				return True
			else:
				print "ERROR - No classifier file"
				self.trained = False
				return False
		except:
			print "ERROR - Error loading classifier"
			self.trained = False
			return False


	

	def save_comparative_classifier(self, fname, fname2):
		if self.comparative_trained:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			f = open(fpath, 'wb')
			pickle.dump(self.comparative_classifier, f)
			f.close()
			return self.save_comparative_type_classifier(fname2)
		else:
			print "ERROR - NOT TRAINED"
			return False


	def load_comparative_classifier(self, fname, fname2):
		try:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			if os.path.isfile(fpath):
				f = open(fpath)
				self.comparative_classifier = pickle.load(f)
				f.close()
				self.comparative_trained = True
				return self.load_comparative_type_classifier(fname2)
			else:
				print "ERROR - No comparative classifier file"
				self.comparative_trained = False
				return False
		except:
			print "ERROR - Error loading comparative classifier"
			self.comparative_trained = False
			return False


	def save_comparative_type_classifier(self, fname):
		if self.trained:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			f = open(fpath, 'wb')
			pickle.dump(self.comparative_type_classifier, f)
			f.close()
			return True
		else:
			print "ERROR - NOT TRAINED"
			return False


	def load_comparative_type_classifier(self, fname):
		try:
			dir = os.path.dirname(__file__)
			fpath = os.path.join(dir, fname)
			if os.path.isfile(fpath):
				f = open(fpath)
				self.comparative_type_classifier = pickle.load(f)
				f.close()
				self.comparative_type_trained = True
				return True
			else:
				print "ERROR - No comparartive type classifier file"				
				return False
		except:
			print "ERROR - Error loading comparartive type classifier"			
			return False


	def clear_scores(self):
		self.feats = {}
		self.scores = {}
		self.weighted_scores = {}
		self.review_count = 0


	def set_product(self, product):
		self.product = product

	def analyze_review(self, review, quality):

		if self.product == "":
			raise Exception("ERROR - product not set.")

		segments = []

		_freq_threshold = 5

		self.review_count += 1

		segments = segments + self._split_segments(review)
		feat_tuple = self._get_feats_and_scores(segments, _freq_threshold, self)
		temp_feats = feat_tuple[0]
		temp_scores = feat_tuple[1]
		temp_weighted_scores = self._scale_scores(copy.deepcopy(temp_scores), quality)
		self.feats = self._union_feats(self.feats, temp_feats)
		self.scores = self._union_scores(self.scores, temp_scores)
		self.weighted_scores = self._union_scores(self.weighted_scores, temp_weighted_scores)



	def dump_scores(self, num):

		_freq_threshold = FREQUENCY_THRESHOLD * num

		if not self.prune_infreq:
			_freq_threshold = 0

		feats_split = self._split_feats(self.feats, _freq_threshold)
		
		feats_freq = feats_split[0]
		feats_infreq = feats_split[1]
		
		return (feats_freq,self.scores,self.weighted_scores)
		

	def classify_and_print(self, reviews):

		freq_feats = {}
		feat_scores = {}
		segments = []

		_freq_threshold = FREQUENCY_THRESHOLD * len(reviews)

		if not self.prune_infreq:
			_freq_threshold = 0

		for review in reviews:
			segments = segments + self._split_segments(review)
			feat_tuple = self._get_feats_and_scores(segments, _freq_threshold, self)
			temp_feats = feat_tuple[0]
			temp_scores = feat_tuple[1]
			freq_feats = self._union_feats(freq_feats, temp_feats)
			feat_scores = self._union_scores(feat_scores, temp_scores)
		
		'''split feats under certain freq'''
		feats_split = self._split_feats(freq_feats, _freq_threshold)
		self.feats_freq = feats_split[0]
		self.feats_infreq = feats_split[1]
		
		for feat in self.feats_freq:
			score = feat_scores.get(feat[0], 0)
			print str(feat[0]) + "\t" + str(feat[1]) + "\t" + str(score) 



	def classify_line(self, line):

		return self._get_feats_and_scores([line], 1, self)[1]


		
	def print_training_data(self, reviews):

		segments = []

		for review in reviews:
			segments = self._split_segments(review)
			training_data = self._get_training_data(segments)
			for element in training_data:
				print element


	
	def train_NaiveBayes(self):
		self.classifier = NaiveBayesClassifier.train(self.training_data)
		self.trained = True



	def train_comparative_NaiveBayes(self):
		self.comparative_classifier = NaiveBayesClassifier.train(self.comparative_training_data)
		self.comparative_type_classifier = NaiveBayesClassifier.train(self.comparative_type_training_data)
		self.comparative_trained = True
		self.comparative_type_trained = True



	def parse_comparative_training_data(self, file):

		dir = os.path.dirname(__file__)
		fpath = os.path.join(dir, file)
		
		with open(fpath, 'r') as f:
			lines = f.readlines()

		i = 0
		while i < len(lines):
			if lines[i][:4] == '<cs-':
				words = nltk.word_tokenize(lines[i+1])
				words_pos = nltk.pos_tag(words)
				bag_of_words = self._make_bag_of_words_pivot(words_pos, COMPARATIVE_DIST_THRESHOLD)
				bag_of_keywords = self._make_bag_of_keywords(words_pos)
				type = int(lines[i][4])
				if type != 1:
					type = 2
				self.comparative_type_training_data.append((bag_of_keywords, type))
				self.comparative_training_data.append((bag_of_words, 1))
				i += 2
			else:
				words = nltk.word_tokenize(lines[i])
				words_pos = nltk.pos_tag(words)
				bag_of_words = self._make_bag_of_words_pivot(words_pos, COMPARATIVE_DIST_THRESHOLD)
				bag_of_keywords = self._make_bag_of_keywords(words_pos)
				if len(bag_of_keywords) > 0:
					self.comparative_training_data.append((bag_of_words, 0))
			i += 1




	def parse_training_data(self, data, sentiment):

		def removeTags(s):
			r = re.compile(r'<[^<>]*>')
			return r.sub('', s)

		for line in data:
			
			line = removeTags(line)

			words = nltk.word_tokenize(line)
			words = nltk.pos_tag(words)
		
			opinion_words = []
			bag_of_words = dict()

			k = OPINION_WORD_DIST_THRESHOLD

			adj_pos = 0

			for word in words:

				adj_pos = adj_pos + 1
				
				if self._qualifies_opinion(word):
					temp_o_set = (word[0], self._get_shifters(words, adj_pos))
					opinion_words.append((temp_o_set,adj_pos))
			
			for o in opinion_words:
				bag_of_words[o[0][0] + str(o[0][1])] = True

			self.training_data.append((bag_of_words, sentiment))


	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	""""""								'''HELPER FUNCTIONS'''								      """"""""
	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
	""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""


	"""_get_training_data"""
	'''
	transforms segments into training data to be manually labeled
	@input - a list of segments
	@output - training data as a list of 3-tuple(feature, (adj, [list of shifters]), adj-feat dist) 
	'''
	def  _get_training_data(self, segments, freq_thresh):

		feats = {}
		scores = {}

		training_data = []

		'''get nouns/noun phrase frequencies'''
		for segment in segments:

			if segment is None:
				continue

			seg_sp = segment.split('##')
			if len(seg_sp) > 1:
				segment = seg_sp[1]
			else:
				seg_sp = [None, None]

			words = nltk.word_tokenize(segment)
			words = nltk.pos_tag(words)
		
			new_feats = []
			noun_set = []
			opinion_words = []
			pos = 0

			'''extract nouns and get their positions'''
			noun_set = self._get_noun_set(words)

			'''get noun phrases'''
			new_feats = self._get_freq_feats(noun_set, feats, scores, freq_thresh)

			'''extract opinion words'''
			opinion_words = self._get_opinion_words(new_feats, words)

			training_data = training_data + ["sentence = " + segment] + ["score = " + str(seg_sp[0])] + opinion_words

		return training_data



	"""_split_segments"""
	'''
	transform a text body into a list of segments
	@input - a text body (i.e a full review)
	@output - a list of segments (simple sentences)
	'''
	def _split_segments(self, text):

		split_segments = []

		sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

		'''tokenize sentences by punctuation'''
		sentences = sent_detector.tokenize(text.strip())

		'''regex'''
		r = re.compile(r',[ ]*but|\n|;')
		
		'''split each sentence conjunctions like but, and, or, etc.'''
		for sentence in sentences:
			if OMIT_PUNCTUATION:	#Omit punctuation if flag ON
				sentence = sentence.translate(string.maketrans("",""), string.punctuation)
			split_text = r.split(sentence)
			for segment in split_text:
				split_segments.append(segment)

		return split_segments




	"""_get_feats_and_scores"""
	'''
	transform a list of segments into a two-tuple output of feats and scores
	@input - a list of sentence segments (from split_segments)
	@output - tuple of:
		feats - a list of features
		scores - a dict with key=feature and val=sentiment score
	'''
	def  _get_feats_and_scores(self, segments, k, analyzer):

		feats = {}
		scores = {}

		'''get nouns/noun phrase frequencies'''
		for segment in segments:
		
			words = nltk.word_tokenize(segment)
			words = nltk.pos_tag(words)
		
			new_feats = []
			noun_set = []
			opinion_words = []
			bag_of_words = dict()
			pos = 0

			'''extract nouns and get their positions'''
			noun_set = self._get_noun_set(words)

			'''get noun phrases'''
			new_feats = self._get_freq_feats(noun_set, feats, scores)

			'''extract opinion words'''
			opinion_words = self._get_opinion_words(new_feats, words)

			'''make bag of opinion words'''
			for tup in opinion_words:
				feat = tup[0]
				bag_of_words[tup[1][0] + str(tup[1][1])] = True

			'''determine if comparative'''
			if self._is_comparative(words):
				self._score_comparative_sentence(words, scores, new_feats)
						
			else:
				for feat in new_feats:	
					scores[feat] = scores.get(feat, 0) + int(self.classifier.classify(bag_of_words))

		'''redundancy prune'''
		for ele in filter(lambda f: len(f.split(" ")) > 1 and feats[f] >= k, feats.keys()):	
			for e in ele.split(" "):
				feats[e] = feats.get(e, 0) - 1
				scores[e] = scores.get(e, 0) - scores.get(ele,0)

		return (feats, scores)



	"""_get_freq_feats"""
	'''
	generate a list of featues in the noun_set scores by frequency
	@input:
		noun_set - set of nouns in split_segments
		feats (passed by ref) - dictionary{feature : frequency}
		scores (passed by ref) - dictionary{feature : score=0} (initializing)
	@output - list of features from the passed noun_set
	@other changes - adds to feats and scores dictionary
	'''
	def _get_freq_feats(self, noun_set, feats, scores):

		new_feats = []

		'''loop through all subsets of nouns in segment'''
		for ele_set in self._nounphrases(noun_set):

			'''compactness prune'''
			if self._isCompact(ele_set) and ele_set != []:

				feat = self._ele_set_to_string(ele_set)
				feats[feat] = feats.get(feat, 0) + 1
				new_feats.append(feat)

		return new_feats


	"""
	Gets the position of the given feature (feat) in the pos_tagged sentence (words)
	Handles noun phrases by taking average position
	"""
	def _get_feat_pos(self, feat, words):

		feat_set = feat.split(' ')
		pos = 0
		sum_pos = 0

		for word in words:
			pos = pos + 1
			if word[0] in feat_set:
				sum_pos = sum_pos + pos

		return sum_pos/len(feat_set)


	"""
	determines if a set of nouns are compact - their distances are all under a threshold
	Returns Boolean
	"""
	def _isCompact(self, ele_set):

		length = len(ele_set);
		for i in xrange(0, length):
			for j in xrange(i+1, length):
				if abs(ele_set[i][1] - ele_set[j][1]) > COMPACTNESS_THRESHOLD:
					return False

		return True	

	"""
	Returns all noun phrases, all subsets of the Powerset(noun_set)
	where all words are within the compactness threshold distance in the noun_set.

	Note that this does mean the nouns are compact in the sentence, this merely 
	eliminates cases where the nouns are definitely not compact to improve run-time.
	O(2^n) vs O(2^k * n)
	This is a generator
	"""
	def _nounphrases(self, nouns):
		k = COMPACTNESS_THRESHOLD+1
		length = len(nouns)
		for i in xrange(0, max(1,length-k+1)):
			end = min(i+k,length)
			for retset in self._powerset(nouns[i:end]):
				yield retset


	"""
	Returns all the subsets of this set. 
	This is a generator.
	WARNING, exponential run time
	"""
	def _powerset(self, seq):

	    if len(seq) <= 1:
	        yield seq
	        yield []
	    else:
	        for item in self._powerset(seq[1:]):
	            yield [seq[0]]+item
	            yield item

	"""
	union of two dictionaries where the value is a number (adds them)
	"""
	def _union_feats(self, feats1, feats2):

		for ele in feats2:
			feats1[ele] = feats1.get(ele, 0) + feats2[ele]

		return feats1

	"""
	union of two dictionaries where the value is a list (merges them)
	"""
	def _union_scores(self, scores1, scores2):

		for ele in scores2:
			scores1[ele] = scores1.get(ele, 0) + scores2[ele]

		return scores1

	"""
	scale each score by given factor
	"""
	def _scale_scores(self, scores, scale_factor):

		for ele in scores:
			scores[ele] = int(round(scores.get(ele, 0) * scale_factor * 2))

		return scores

	"""
	transforms a list of 2-tuple elements into a space delimited string of the first element of tuple 
	"""
	def _ele_set_to_string(self, ele_set):
		feat = ""
		for ele in ele_set:
			feat = feat + ele[0] + " "
		feat = feat[:-1]
		return feat


	"""
	split feature set into features with freq > theshold and < threshold
	"""
	def _split_feats(self, feats, threshold):

		if not self.prune_infreq:
			threshold = 0

		sorted_feats = sorted(feats.iteritems(), key=operator.itemgetter(1))

		ind = self._find_cutoff(sorted_feats, threshold)
		
		if ind == -1:
			return (sorted_feats, []) 
		else:
			return (sorted_feats[:ind], sorted_feats[ind:])


	"""
	find the cutoff element in dictonary with binary search
	"""
	def _find_cutoff(self, feats, threshold):

		mid = len(feats)/2

		if mid < 1:
			return -1

		if feats[mid][1] < threshold and feats[mid-1][1] >= threshold:
			return mid

		elif feats[mid][1] < threshold:
			return self._find_cutoff(feats[:mid], threshold)

		else:
			return self._find_cutoff(feats[mid:], threshold)



	"""_get_opinion_words"""
	'''
	associates each feat with opinion words and related words
	@input:
		feats - a list of features
		words - the pos_tagged sentence
	@output - opinion word data as a list of 3-tuple(feature, (adj, [list of shifters]), adj-feat dist) 
	'''
	def _get_opinion_words(self, feats, words):

		k = OPINION_WORD_DIST_THRESHOLD

		opinion_words = []

		for feat in feats:

			feat_pos = self._get_feat_pos(feat, words)
			adj_pos = 0

			for word in words:

				adj_pos = adj_pos + 1
				dist = abs(adj_pos - feat_pos)
				
				if self._qualifies_opinion(word) and dist <= k:
					adj_dist = dist
					temp_o_set =  (word[0], self._get_shifters(words, adj_pos))
					opinion_words.append((feat,temp_o_set,adj_dist))

		return opinion_words


	"""_get_noun_set"""
	'''
	gets the set of all nouns in a sentence
	@input - a pos_tagged sentence
	@output - a list of nouns
	'''
	def _get_noun_set(self, words):

		noun_set = []
		pos = 0

		for word in words:
			pos = pos + 1
			if word[1] == 'NN' or word[1] == 'NNS' or word[1] == 'NNP' or word[0].lower() in badwords.isNN:
				if badwords.valid(word[0]):
					noun_set.append((word[0].lower(), pos))
			elif word[0].lower() == 'it':
				noun_set.append((word[0].lower(), pos))

		return noun_set



	"""_get_shifters"""
	'''
	gets all relevant sentiment shifters (see _is_shifter)
	@input:
		words - a pos_tagged sentence
		adj_pos - position of the adjective we are connecting
	@output - a list of sentiment shifters 
	'''
	def _get_shifters(self, words, adj_pos):

		k = SHIFTER_DIST_THRESHOLD

		return_set = []
		shifter_pos = 0

		for word in words:
			shifter_pos = shifter_pos + 1
			if self._is_shifter(word[0]) and abs(shifter_pos-adj_pos) < k:
				return_set.append(word[0])

		return return_set


	"""_is_shifter"""
	'''
	determines if the word is a sentiment shifter
	@input - a word (string)
	@output - boolean value
	'''
	def _is_shifter(self, word):
		'''
		Words that change opinion orientation:
		Common examples are negation words: not, never, cannot, etc.
		Also modal auxiliary verbs: would, should, could
		Presuppositional items: barely, hardly
		Verbs like: fail, omit, neglect
		Sarcasm
		'''
		word = word.lower()

		if word in ['isnt', 'doesnt', 'cant']:	#typos
			return True

		if word in lexicons.but_words:
			return True

		if word in lexicons.negation_words:
			return True

		if word in lexicons.other_shifters:
			return True

		if word in lexicons.presuppositional_words:
			return True

		if word in lexicons.modular_auxilary_words:
			return True

		return False



	def _qualifies_opinion(self, word):

		if (word[1] == 'JJ' or word[1] == 'JJS' or word[1] == 'JJR' or word[1] == 'RB' or word[1] == 'RBJ' or word[1] == 'RBS'):
			if word[0].lower() not in badwords.badJJ:
				return True

		"""
		correct unforgivable pos tag errors
		"""
		if word[0].lower() in ['fun', 'poor', 'ideal']:
			return True

		"""
		Mark opinion qualifying verbs
		"""
		if word[1][:2] == 'VB':
			if word[0].lower() in ['love', 'loves', 'sucks', 'hate']:
				return True

		return False


	def _classify_comparative_type(self, words_pos):
		if self.comparative_type_trained:
			keywords = self._make_bag_of_keywords(words_pos)
			return self.comparative_type_classifier.classify(keywords)
		else:
			raise Exception("ERROR - Comparative Type Classifier Not Trained")


	def _potentially_comparative(self, words_pos):

		for pair in words_pos:
			if self._word_is_comparative(pair):
				return True

		return False

	def _word_is_comparative(self, pair):
		if pair[1] in ['JJS', 'RBR', 'RBS', 'JJR']:
			return True
		elif pair[0] in lexicons.comparative_keywords:
			return True
		else:
			return False

	def _make_bag_of_keywords(self, words):
		return_dict = {}
		for word in words:
			if self._word_is_comparative(word):
				return_dict[word] = True
		return return_dict

	def _make_bag_of_words(self, words):
		return_dict = {}
		for word in words:
			return_dict[word] = True
		return return_dict

	def _make_bag_of_words_pivot(self, words, r):
		return_dict = {}
		pivot = 0
		for word in words:
			if self._word_is_comparative(word):
				pos = 0
				for word in words:
					if abs(pos-pivot) <= r:
						return_dict[word] = True
					pos += 1
			pivot += 1
		return return_dict


	def _get_keyword_pos(self, words_pos):
		pos = 0
		for pair in words_pos:
			if self._word_is_comparative(pair):
				yield (pair, pos)
			pos += 1


	def _get_comparative_feats(self, new_feats, words, pivot, k):
		'''determine feats relevant in the comparison'''
		left_words = []
		right_words = []
		for feat in new_feats:
			feat_pos = self._get_feat_pos(feat, words)
			if 0 < (feat_pos - pivot) <= k:
				right_words.append(feat)
			elif 0 > (feat_pos - pivot) >= -k:
				left_words.append(feat)
		return (left_words, right_words)


	def _is_comparative(self, words_pos):

		if not self.comparative_trained:
			raise Exception("ERROR - Comparative Classifier Not Trained")

		if self._potentially_comparative(words_pos):
			bag_of_words = self._make_bag_of_words_pivot(words_pos, COMPARATIVE_DIST_THRESHOLD)
			is_comp = self.comparative_classifier.classify(bag_of_words)
			return (is_comp == 1)
		else:
			return False


	def _score_comparative_sentence(self, words, scores, new_feats):

		'''classify as non-equal, superlative, equative'''
		comp_type = self._classify_comparative_type(words)

		'''treat each keyword as a case'''
		'''ALTERNATIVE - only handle one keyword, must pick the best one'''
		for tup in self._get_keyword_pos(words):

			word = tup[0][0]
			pivot = tup[1]

			bag_of_words = self._make_bag_of_words_pivot(words, COMPARATIVE_DIST_THRESHOLD)

			tup = self._get_comparative_feats(new_feats, words, pivot, COMPARATIVE_DIST_THRESHOLD)
			
			left_words = tup[0]
			right_words = tup[1]

			sent = int(self.classifier.classify(bag_of_words))  #may need to do more for comp_type 1
                                                                                                                                                                                            
			'''now we have the keyword, the left and right word, and the type'''
			if comp_type == 1:	#non-equal
				'''score greater / lesser left->right'''
				for left_word in left_words:
					scores[left_word] = scores.get(left_word, 0) + sent * 1
				for right_word in right_words:
					scores[right_word] = scores.get(right_word, 0) + sent * -1
				
			elif comp_type == 2:	#equative
				'''treat as normal sent'''
				for feat in new_feats:	
					scores[feat] = scores.get(feat, 0) + sent

			else:
				raise Exception("Invalid comparative type")





class ptr:
    def __init__(self, obj): self.obj = obj
    def get(self):    return self.obj
    def set(self, obj):      self.obj = obj