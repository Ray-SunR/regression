__author__ = 'Renchen'

import os.path
import hashlib
import re
import json
import regression
from bson.binary import Binary

class Base(object):
	def __init__(self):
		self._impl = {}
	def set(self, key, val):
		assert key in self._impl
		self._impl[key] = val

	def get(self, key):
		assert key in self._impl
		return self._impl[key]

	def obj(self):
		return self._impl


class Document(Base):

	def __init__(self):
		self._impl = {
			'hash': '',# PK
			'document_name': '', # name for the document
			'ext': '', # extension
			'path': '', # path for the document
			'benchmarks': [ ] # list(Benchmarks)
		}

	def populate(self, d_path):
		filename,ext = os.path.splitext(d_path)
		self.set('document_name', os.path.basename(d_path))
		self.set('ext', ext)
		self.set('path', d_path)
		try:
			with open(d_path) as file:
				sha1 = hashlib.sha1(file.read())
				self.set('hash', sha1.hexdigest())
		except Exception as e:
			print(e)


class Run(Base):
	def __init__(self):
		self._impl = {
			'document_name':'', # document name
			'version':'', #version
			'hash': '', # document hash
			'pages': [], # list(Page_id)
			'diffs': [] # list(Difference_id)
		}


class Benchmark(Base):
	def __init__(self):
		self._impl = {
			'version': '', # PK
			'type': '', # sdk or pdf2image or docpub etc...
			'runs': [] # list(Run_id)
		}

	# make sure the dname has no extension!
	def populate(self, regression, document, collections):
		pattern = os.path.splitext(document.get('document_name'))[0] + '.*_(\d+)\.png'

		for file in regression.RefOutFilePaths():
			ret = re.search(pattern, file)
			if not ret:
				continue

			run = Run()
			dname = document.get('document_name')
			hash = document.get('hash')
			run.set('version', self.get('version'))
			run.set('document_name', dname)
			run.set('hash', hash)

			page = Page()

			page.set('hash', hash)
			page.set('version', self.get('version'))
			page.set('document_name', dname)
			page.set('page_num', ret.group(1))
			page.set('ext', 'png')
			with open(file, 'r') as mfile:
				page.set('binary', Binary(mfile.read()))

			page_inserted = collections['pages'].insert_one(page.obj())

			diff_page = Page()
			diff_page.set('version', regression.GetTarVersion())
			diff_page.set('document_name', dname)
			diff_page.set('hash', hash)
			diff_page.set('page_num', ret.group(1))
			diff_page.set('ext', 'png')

			assert os.path.basename(file) in regression.RefOutDiffMap()
			diff_page_path = regression.RefOutDiffMap()[os.path.basename(file)]
			with open(diff_page_path, 'r') as mfile:
				diff_page.set('binary', Binary(mfile.read()))

			diff_page_inserted = collections['pages'].insert_one(diff_page.obj())

			metrics = regression.DiffMetrics()
			assert os.path.basename(file) in metrics
			metric = metrics[os.path.basename(file)]
			metric.set('page_num', ret.group(1))
			metric.set('page', diff_page_inserted.inserted_id)

			diff = Difference()
			diff.set('version', regression.GetTarVersion())
			diff.set('metrics', metric.obj())

			diff_inserted = collections['differences'].insert_one(diff.obj())

			found_run = collections['runs'].find_one({'version': run.get('version'), 'hash': run.get('hash')})
			run.get('pages').append(page_inserted.inserted_id)
			run.get('diffs').append(diff_inserted.inserted_id)

			run_inserted = collections['runs'].update_one({'version': run.get('version'), 'hash': run.get('hash')}, {'$push': {'pages': page_inserted.inserted_id, 'diffs': diff_inserted.inserted_id}}, upsert=True)

			# only push run to benchmarks if it's new
			if run_inserted.upserted_id:
				# only push to runs if the run is inserted
				benchmark_inserted = collections['benchmarks'].update_one({'version': self.get('version')}, {'$push': {'runs': run_inserted.upserted_id}}, upsert=True)

				# only push benchmarks if it's new
				if benchmark_inserted.upserted_id:
					collections['documents'].update_one({'hash': hash}, {'$push': {'benchmarks': benchmark_inserted.upserted_id}})


class Page(Base):
	def __init__(self):
		self._impl = {
			'version': '', # PK version number
			'hash': '', # document hash
			'document_name': '', # document name
			'document_hash': '', #FK document hash
			'page_num': '', # page number
			'binary': '', # binary data for the page
			'ext': '', # extension
		}

class Difference(Base):
	def __init__(self):
		self._impl = {
			'version': '', # The target version
			'metrics' : {}, #page_num -> DifferenceMetric
		}

class DifferenceMetric(Base):
	def __init__(self, obj=None):
		Base.__init__(self)
		self._impl = {
			'diff_percentage': '', # Difference percentage
			'page_num': '' , # page number
			'page': '', # page_id
		}
		if obj:
			assert isinstance(obj, dict)
			self.set('page_num', obj['page_num'])
			self.set('diff_percentage', obj['diff_percentage'])
			self.set('page', obj['page'])


class JsonEncoder(json.JSONEncoder):
	def default(self, o):
		return o.obj()