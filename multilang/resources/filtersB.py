#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import string
import unicodedata
import storm


# Le arquivo com Stopwords
def read_stopwords (filein):
    f = open(filein, 'r')
    stopwords = f.readline().split(', ')
    f.close()
    return stopwords

    
# Remove sinais de pontuaçao STRING --> STRING
def filter_punct (ent):
	punct = re.compile('[%s]' % string.punctuation.replace('#', '').replace('@', ''))  #Tags precisam ser mantidas, bem como @ 
	ent = punct.sub('', ent)
	
	return ent

# Remover caracteres repetidos em excesso, com o "o" em gooooooooooooooooool STRING --> STRING
def filter_charRepetition (ent):
	expRepeticao1 = re.compile('^([rs])\\1')
	expRepeticao2 = re.compile('([rs])\\1$')
	expRepeticao3 = re.compile('([^rs])\\1{1,}')
	expRepeticao4 = re.compile('([\\S\\s])\\1{2,}')
	
	ent = expRepeticao4.sub('\\1\\1', ent)
	ent = expRepeticao3.sub('\\1', ent)
	ent = expRepeticao2.sub('\\1', ent)
	ent = expRepeticao1.sub('\\1', ent)
	
	return ent
	
# Remove URL STRING --> STRING
def filter_url (ent):
	urlRef = re.compile("((https?|ftp):[\/]{2}[\w\d:#@%/;\$()~_?\+-=\\\.&]*)")
	
	ent = urlRef.sub('', ent)
	
	return ent

# Remove palavras da string STRING --> STRING
def filter_keywords(ent, keywords):
    for kw in keywords:
        userkw = '@'+kw+' '
        hashtagkw = '#'+kw+ ' '
        normalkw = ' '+kw+' '

        ent = ent.replace(userkw, ' ')
        ent = ent.replace(hashtagkw, ' ')
        ent = ent.replace(normalkw, ' ')
	return ent


# Retorna um SET contendo as N-gramas do texto STRING --> SET
def gen_NGrams(N,text, stopwords, ignore_stops = True, create_subgrams=True, ngram_sep=''):
	NList = [] # start with an empty list
	if N > 1:
		partes = text.split() + (N *[''])
	else:
		partes = text.split()
	# append the slices [i:i+N] to NList
	for i in range(len(partes) - (N - 1) ):
		NList.append(partes[i:i+N])

	result = set()
	for item in NList:
		if create_subgrams:
			list_iterations = xrange(1, N + 1)
		else:
			list_iterations = [N]
		for i in list_iterations:
			stops_found = [x for x in item[0:i] if x in stopwords or x == ""]
			#Ignora N-gramas so com stop words
			dado = ngram_sep.join(item[0:i])
			if ngram_sep.join(stops_found) != dado or ignore_stops == False:
				if dado != ngram_sep:
					result.add(dado)
	return result
	
# Filtra Acentos String --> String
def filter_accents(s):
   s = unicode(s)
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))
 
#Filtra Stopwords Set --> Set
def filter_stopwords(gramsSet, stopwords):
    return gramsSet - stopwords
 
#Filtra numeros sozinhos Set --> Set
def filter_numbers(gramsSet):
	return [item for item in gramsSet if not item.isdigit()]
	
#Filtra termos menores que min_size  set--> Set
def filter_small_words(gramsSet, min_size):
	return [item for item in gramsSet if len(item)>= min_size]

def filter_load_stopwords(stopwordsFile):
    f = open(stopwordsFile, 'r')

    linha= f.readline().strip('\n')
    linha = linha.split(", ")
    return set(linha)


def cleanText(string, stopwords=[], keywords=[]):
    stopwords = set(stopwords)
    out = filter_punct(string)
    out = out.lower()
    out = filter_charRepetition(out)
    out = filter_url (out)
    out = filter_accents(out)
#    print out
#    out = filter_keywords(out, keywords)
#    print out
#    raw_input()
    out = gen_NGrams(3, out, stopwords, ngram_sep='_')
    out = filter_stopwords(out, stopwords)
    out = filter_numbers(out)
    out = filter_small_words(out, 3)
    return ' '.join(out)


class StreamFilterBolt(storm.BasicBolt):
    def process(self, tup):
        decoded = simplejson.loads(tup.values[0])
        storm.emit(cleanText(decoded['text']))

StreamFilterBolt().run()