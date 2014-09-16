
#!/usr/bin/python

notNN = [
	'great', 'better', 'good', 'cool', 'sexy', 'nice', 'super', 'amazing', 'excellent', 'fast', 
	'bad', 'terrible', 'poor', 'anyway', 'cant', 'not', 'wont', 'awesome', 'buggy', 'slick', 'isnt'
	'w', 'ive', 'superb', 'perfect', 'doesnt', 'simple', 'dont', 'make', 'isnt', 'very',
	'+', ')', '(', '%'
]
badNN = [
	'everything', 'nothing', 'anything', 'none', 'etc', 'ease', 'problem', 'im', 'love', 'everyday', 'fun', 'crap', 'reason', 'ideal', 'sucks', 
	'way', 'lot'
]

badJJ = [
	'n\'t', 'not', 'however', 'sometimes', 'just', 'then', 'such', 'got', 'even', 'also'
]

isNN = [
	'screen'
]


def valid(word):
	result1 = word.lower() not in notNN
	result2 = word.lower() not in badNN
	return result1 and result2



	