__author__ = 'Renchen'

import os.path
import hashlib
import re
import regression
class Base(object):
	def set(self, key, val):
		assert key in self.__impl
		self.__impl[key] = val

	def get(self, key):
		assert key in self.__impl
		return self.__impl[key]

	def obj(self):
		return self.__impl

class Document(Base):

	def __init__(self):
		self.__impl = {
			'hash': '',# PK
			'document_name': '', # name for the document
			'ext': '', # extension
			'path': '', # path for the document
			'benchmarks': [ ] # list(Benchmarks)
		}

	def populate(self, d_path):
		filename,ext = os.path.splitext(d_path)
		self.set('document_name', filename)
		self.set('ext', ext)
		self.set('path', d_path)
		try:
			sha1 = hashlib.sha1
			with open(d_path) as file:
				sha1.update(file.read())
				self.set('hash', sha1.hexdigest())
		except Exception as e:
			print(e)

class Benchmark(Base):
	def __init__(self):
		self.__impl = {
			'version': '', # PK
			'type': '', # sdk or pdf2image or docpub etc...
			'parent': '', # FK (Document.hash)
			'pages': [], # list(Page)
			'diffs': [], # list(Difference)
			'document_name': '' # document name
		}

	def populate(self, regression):
		pattern = self.get('document_name') + '.*_(\d+)\.png'
		for file in regression.RefOutFilePaths():
			ret = re.match(pattern, file)
			if ret:
				page = Page()
				page.set('version', self.get('version'))
				page.set('document_name', self.get('document_name'))
				page.set('page_num', ret.group(1))
				page.set('ext', 'png')
				with open(file, 'r') as mfile:
					page.set('binary', mfile.read())

				diff_page = Page()
				diff_page.set('version', regression.GetTarVersion())
				diff_page.set('document_name', self.get('document_name'))
				diff_page.set('page_num', ret.group(1))
				diff_page.set('ext', 'png')

				assert file in regression.RefOutDiffMap()
				diff_page_path = regression.RefOutDiffMap()[file]
				with open(diff_page_path, 'r') as mfile:
					diff_page.set('binary', mfile.read())

				metrics = regression.DiffMetrics()
				assert os.path.basename(file) in metric
				metric = metrics[os.path.basename(file)]
				metric.set('page_num', ret.group(1))
				metric.set('page', diff_page)

				diff = Difference()
				diff.set('version', regression.GetTarVersion())
				diff.set('metrics', metric)

				self.get('pages').append(page)
				self.get('diffs').append(diff)
			else:
				assert False


class Page(Base):
	def __init__(self):
		self.__impl = {
			'version': '', # PK version number
			'document_name': '', # document name
			'page_num': '', # page number
			'binary': '', # binary data for the page
			'ext': '', # extension
		}

class Difference(Base):
	def __init__(self):
		self.__impl = {
			'version': '', # The target version
			'metrics' : {}, #page_num -> DifferenceMetric
		}

class DifferenceMetric(Base):
	def __init__(self):
		self.__impl = {
			'diff_percentage': '', # Difference percentage
			'page_num': '' , # page number
			'page': '', # page
		}