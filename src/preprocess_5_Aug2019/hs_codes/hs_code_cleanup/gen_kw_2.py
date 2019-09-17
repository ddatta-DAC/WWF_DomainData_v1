# ---------------------------------------- #
# This script takes in the csv with the combined full description with the notations
# Then processes each description as an infix arithmetic notation ( e.g [A+[B+[C+D]]] ) using a stack
# It separates the positive and negatve elements, and for description extracts the tokens.
# Lemmatization is done, after which keywords are extracted - which may be single nouns or maybe phrases
# --------------------------------------- #

import spacy
import textacy
import textacy.keyterms
nlp = spacy.load('en')
import pandas as pd
from pprint import pprint
import textacy.preprocess
from itertools import permutations



def extract_n_grams(text_sent, n=1):
    exclude_pos = [
        'SYM',
        'NUM',
        '$',
        'VBG',
        'ADV',
        'IN',
        'CONJ',
        'PUNCT',
        'ADP',
        'VERB',
        'CC',
        'PART',
        'INTJ',
        'JJR',
        'JJS'
    ]
    sent = textacy.preprocess.normalize_whitespace(text_sent)
    doc = textacy.Doc(sent, lang='en')
    ng = textacy.extract.ngrams(
        doc,
        n=n,
        exclude_pos=exclude_pos
    )
    ngs = []
    for _ng in ng:
        ngs.append(str(_ng))
    ngs = list(set(ngs))

    # process the unigrams and bigrams
    # remove extra spaces
    # remove space with a hyphen
    res = []
    for item in ngs:
        item = item.strip(' ')
        item = item.replace('-', ' ')
        item = textacy.preprocess.normalize_whitespace(item)
        res.append(item)
    res = list(set(res))
    return res


def get_keywords(text_sent):
    _unigrams = extract_n_grams(text_sent, 1)
    _bigrams = extract_n_grams(text_sent, 2)

    rmv_list = []
    # check if 2 unigrams are in a bigram
    for comb in list(permutations(_unigrams, 2)):
        tentative = ' '.join(comb)
        if tentative in _bigrams:
            for c in comb:
                rmv_list.append(c)
    rmv_list = list(set(rmv_list))

    for r in rmv_list:
        if r in _unigrams:
            _unigrams.remove(r)
    tmp = list(_unigrams)
    _unigrams = []


    for x in tmp:
        if len(x) > 1:
            _unigrams.append(x)

    kws = []
    kws.extend(_unigrams)
    kws.extend(_bigrams)
    return kws


def process_line(tokens):
    stack = []
    is_neg = False

    pos_kws = []
    neg_kws = []

    for i in range(len(tokens)):
        # Lemmatization
        t = str(tokens[i].lemma_)
        is_neg = False

        if t == "[" or t == "[[":
            stack.append(t)
        elif t == "]":
            cs = []
            while (len(stack) > 0):  # while stack is not empty
                z = stack.pop()
                if z == '[':
                    break
                else:
                    cs.append(z)

            cs = list(reversed(cs))
            sent = ' '.join(cs)
            sent = sent.replace(';', ',')
            sent = sent + '.'
            kws = get_keywords(sent)

            # if len(kws) > 0:
            #     print(kws)

            if len(stack) > 0 and stack[-1] == '!':
                is_neg = True
                stack.pop()  # remove the !

            if kws is not None and len(kws) > 0:
                if is_neg:
                    neg_kws.extend(kws)
                else:
                    pos_kws.extend(kws)

        elif t == '&':
            stack.append(t)
        elif t == '!':
            stack.append(t)
        else:
            stack.append(t)

    pos_kws = list(set(pos_kws))
    neg_kws = list(set(neg_kws))
    pos_kws = ';'.join(pos_kws)
    neg_kws = ';'.join(neg_kws)

    return pos_kws, neg_kws


def get_df_kws(row):
    z = row['full_desc']
    if z is None or type(z)!= str:
        row['pos_kw'] = None
        row['neg_kw'] = None
    else:
        z = z.replace(';', '&;')
        nlp = spacy.load('en')
        doc = nlp(z)
        tokens = [t for t in doc]
        pos_kw, neg_kw = process_line(tokens)
        # print(' ||+|| ', pos_kw)
        # print(' ||-|| ', neg_kw)
        row['pos_kw'] = pos_kw
        row['neg_kw'] = neg_kw
    return row

def add_in_kws(df):
    df = df.apply(get_df_kws, axis=1)
    return df
