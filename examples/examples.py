import math
import re

import ComputeGraph

file_name = 'text_corpus.txt'

def word_count():

    def split_text(row):
        for word in re.split(r'[^a-zA-Z]', row['text']):
            if len(word) > 0:
                yield {'doc_id' : row['doc_id'], 'word': word.lower()}

    def count_words(rows):
        yield {'word' : rows[0]['word'], 'number' : len(rows)}

    graph = ComputeGraph.ComputeGraph('WordCounter') \
        .map(split_text) \
        .sort('word') \
        .reduce(count_words, 'word')

    graph.run({'WordCounter': file_name}, output='output1.txt')


def tf_idf():

    def split_text(row):
        for word in re.split(r'[^a-zA-Z]', row['text']):
            if len(word) > 0:
                yield {'doc_id' : row['doc_id'], 'number_of_docs' : row['number_of_docs'], 'word': word.lower()}

    def get_frequency(rows):
        yield {**rows[0], 'frequency' : len(rows)}

    def count_docs_with_word(rows):
        for row in rows:
            yield {**row, 'number_of_docs_with_word': len(rows)}

    def count_docs(rows):
        for row in rows:
            yield {**row, 'number_of_docs' : len(rows)}

    def get_top_3(rows):
        docs = []
        for row in rows:
            tf_idf = row['frequency'] / row['words_in_doc'] * math.log(row['number_of_docs']/row['number_of_docs_with_word'])
            docs.append((row['doc_id'], tf_idf))
        docs.sort(key=lambda x : x[1], reverse=True)
        yield {'word' : rows[0]['word'], 'index' : docs[:3]}

    def count_all_words_in_doc(rows):
        for row in rows:
            yield {**row, 'words_in_doc' : len(rows)}

    count_tf_idf = ComputeGraph.ComputeGraph('main') \
        .reduce(count_docs) \
        .map(split_text) \
        .sort('doc_id') \
        .reduce(count_all_words_in_doc, key='doc_id') \
        .sort(key=('word', 'doc_id')) \
        .reduce(get_frequency, key=('word', 'doc_id')) \
        .reduce(count_docs_with_word, 'word') \
        .reduce(get_top_3, 'word')

    count_tf_idf.run({'main' : file_name}, output='output2.txt')


def count_pmi():

    def count_docs(rows):
        for row in rows:
            yield {**row, 'number_of_docs' : len(rows)}

    def split_text(row):
        for word in re.split(r'[^a-zA-Z]', row['text']):
            if len(word) > 4:
                yield {'doc_id': row['doc_id'], 'number_of_docs': row['number_of_docs'], 'word': word.lower()}

    def count_words_in_doc(rows):
        yield {**rows[0], 'number_in_doc': len(rows)}

    def count_words(rows):
        good_word = True
        if len(rows) < rows[0]['number_of_docs']:
            good_word = False
        for row in rows:
            if row['number_in_doc'] < 2:
                good_word = False
        if good_word:
            total_number = sum(row['number_in_doc'] for row in rows)
            for row in rows:
                yield {**row, 'total_number' : total_number}

    def count_all_words_in_doc(rows):
        number = sum(row['number_in_doc'] for row in rows)
        for row in rows:
            yield {**row, 'words_in_doc' : number}

    def count_all_words(rows):
        number = sum(row['words_in_doc'] for row in rows)
        for row in rows:
            yield {**row, 'words_in_total': number}

    def count_pmi(rows):
        words = []
        for row in rows:
            pmi = math.log(row['number_in_doc'] * row['words_in_total']/row['total_number']/row['words_in_doc'])
            words.append((row['word'], pmi))
        words.sort(key=lambda x : x[1], reverse=True)
        yield {'doc_id' : rows[0]['doc_id'], 'top_words' : words[:10]}

    graph = ComputeGraph.ComputeGraph('main') \
        .reduce(count_docs) \
        .map(split_text) \
        .sort(key=('word', 'doc_id')) \
        .reduce(count_words_in_doc, key=('word', 'doc_id')) \
        .reduce(count_words, key=('word')) \
        .sort(key='doc_id') \
        .reduce(count_all_words_in_doc, key='doc_id') \
        .reduce(count_all_words) \
        .reduce(count_pmi, key='doc_id')

    graph.run({'main' : file_name}, output='output3.txt')

def test_all():
    word_count()
    tf_idf()
    count_pmi()

if __name__ == '__main__':
    word_count()
    tf_idf()
    count_pmi()