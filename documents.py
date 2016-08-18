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

	def serialize(self, bson):
		for key in self._impl.keys():
			if isinstance(self._impl[key], str):
				bson[key] = self._impl[key]
			elif isinstance(self._impl[key], float):
				bson[key] = self._impl[key]
			elif isinstance(self._impl[key], Base):
				obj = {}
				self._impl[key].serialize(obj)
				bson[key] = obj
			elif isinstance(self._impl[key], list):
				lobj = []
				for item in self._impl[key]:
					obj = {}
					if isinstance(item, Base):
						item.serialize(obj)
					else:
						obj = self._impl[key]
					lobj.append(obj)
				bson[key] = lobj
			else:
				assert False

	def bson(self, collections):
		pass

	def __dummy_copy(self):
		pass


class Document(Base):

	def __init__(self):
		self._impl = {
			'hash': '',# PK
			'document_name': '', # name for the document
			'ext': '', # extension
			'path': '', # path for the document
			'benchmarks': [ ] # list(Benchmarks)
		}

	def bson(self, collections):
		benchmark_ids = []
		for benchmark in self.get('benchmarks'):
			assert isinstance(benchmark, Base)
			id = benchmark.bson(collections)
			benchmark_ids.append(id)

		obj = self.__dummy_copy() 
		obj['benchmarks'] = benchmark_ids
		found = collections['documents'].find_one({'hash': self.get('hash')})
		if found:
			collections['documents'].update_one({'hash': self.get('hash')}, {'$push': {'benchmarks': { '$each' : benchmark_ids}}})
			return found['_id']
		else:
			inserted = collections['documents'].insert_one(obj)
			return inserted.inserted_id

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

	def __dummy_copy(self):
		doc = Document()
		ret = doc.obj()
		ret['hash'] = self.get('hash')
		ret['document_name'] = self.get('document_name')
		ret['ext'] = self.get('ext')
		ret['path'] = self.get('path')
		return ret

class Run(Base):
	def __init__(self):
		self._impl = {
			'document_name':'', # document name
			'version':'', #version
			'hash': '', # document hash
			'pages': [], # list(Page_id)
			'diffs': [] # list(Difference_id)
		}

	def populate(self, document):
		self.set('document_name', document.get('document_name'))
		self.set('hash', document.get('hash'))

	def bson(self, collections):
		page_ids = []
		diff_ids = []
		for page in self.get('pages'):
			assert isinstance(page, Base)
			id = page.bson(collections)
			page_ids.append(id)

		for diff in self.get('diffs'):
			assert isinstance(diff, Base)
			id = diff.bson(collections)
			diff_ids.append(id)

		obj = self.__dummy_copy()
		
		obj['pages'] = page_ids
		obj['diffs'] = diff_ids
		
		found = collections['runs'].find_one({'version': self.get('version'), 'document_name': self.get('document_name'), 'hash': self.get('hash')})
		if found:
			collections['runs'].update_one({'version': self.get('version'), 'document_name': self.get('document_name'), 'hash': self.get('hash')}, {'$push': {'$each': {'pages': page_ids, 'diffs': diff_ids}}})
			return  found['_id']
		else:
			inserted = collections['runs'].insert_one(obj)
			return inserted.inserted_id
		
	def __dummy_copy(self):
		run = Run()
		ret = run.obj()
		ret['document_name'] = self.get('document_name')
		ret['version'] = self.get('version')
		ret['hash'] = self.get('hash')
		return ret


class Benchmark(Base):
	def __init__(self):
		self._impl = {
			'version': '', # PK
			'type': '', # sdk or pdf2image or docpub etc...
			'hash': '', # pk, document hash
			'runs': [] # list(Run_id)
		}

	def bson(self, collections):
		run_ids = []
		for run in self.get('runs'):
			assert isinstance(run, Base)
			runid = run.bson(collections)
			run_ids.append(runid)

		found = collections['benchmarks'].find_one({'version': self.get('version'), 'hash': self.get('hash')})
		if found:
			collections['benchmarks'].update_one({'_id': found['_id']}, {'$push': {'runs': {'$each': run_ids}}})
			return found['_id']
		else:
			obj = self.__dummy_copy()
			obj['runs'] = run_ids
			inserted = collections['benchmarks'].insert_one(obj)
			return inserted.inserted_id
			
	def __dummy_copy(self):
		bm = Benchmark()
		ret = bm.obj()
		ret['version'] = self.get('version')
		ret['type'] = self.get('type')
		ret['hash'] = self.get('hash')
		return ret

	def populate(self, regression, document, run):
		# remember: the output generated from binary (pdf2image) for single page pdf will not include the page number
		pattern = os.path.splitext(document.get('document_name'))[0] + '(?:\.png|_(\d+).png)'

		dname = document.get('document_name')
		hash = document.get('hash')
		self.set('hash', hash)
		for file in regression.RefOutFilePaths():
			ret = re.search(pattern, file)
			if not ret:
				continue

			page = Page()
			run.get('pages').append(page)

			page.set('hash', hash)
			page.set('version', self.get('version'))
			page.set('document_name', dname)
			page.set('page_num', ret.group(1))
			page.set('ext', 'png')
			with open(file, 'r') as mfile:
				page.set('binary', Binary(mfile.read()))
				page.set('path', file)


			metrics = regression.DiffMetrics()
			assert os.path.basename(file) in metrics
			metric = metrics[os.path.basename(file)]
			metric.set('version', regression.GetTarVersion())
			metric.set('hash', hash)
			run.get('diffs').append(metric)

			diff_page = Page()
			metric.set('page', diff_page)
			metric.set('page_num', ret.group(1))

			diff_page.set('version', regression.GetTarVersion())
			diff_page.set('document_name', dname)
			diff_page.set('hash', hash)
			diff_page.set('page_num', ret.group(1))
			diff_page.set('ext', 'png')

			assert os.path.basename(file) in regression.RefOutDiffMap()
			diff_page_path = regression.RefOutDiffMap()[os.path.basename(file)]
			with open(diff_page_path, 'r') as mfile:
				diff_page.set('binary', Binary(mfile.read()))
				diff_page.set('path', diff_page_path)


class Page(Base):
	def __init__(self):
		self._impl = {
			'version': '', # PK version number
			'hash': '', # document hash
			'document_name': '', # document name
			'page_num': '', # page number
			'binary': '', # binary data for the page
			'ext': '', # extension
			'path': ''
		}

	def bson(self, collections):
		found = collections['pages'].find_one({'hash': self.get('hash'), 'page_num': self.get('page_num'), 'version': self.get('version')})
		if found:
			collections['pages'].update_one({'hash': self.get('hash'), 'page_num': self.get('page_num'), 'version': self.get('version')}, {'$set': {'binary': Binary(self.get('binary'))}})
			return found['_id']
		else:
			obj = self.__dummy_copy()
			obj['binary'] = Binary(obj['binary'])
			inserted = collections['pages'].insert_one(obj)
			return inserted.inserted_id
			
	def __dummy_copy(self):
		page = Page()
		ret = page.obj()
		ret['version'] = self.get('version')
		ret['hash'] = self.get('hash')
		ret['document_name'] = self.get('document_name')
		ret['page_num'] = self.get('page_num')
		ret['ext'] = self.get('ext')
		ret['path'] = self.get('path')
		return ret

	def serialize(self, obj):
		obj['version'] = self._impl['version']
		obj['hash'] = self._impl['hash']
		obj['document_name'] = self._impl['document_name']
		obj['page_num'] = self._impl['page_num']
		#obj['binary'] = self._impl['binary']
		obj['ext'] = self._impl['ext']


class DifferenceMetric(Base):
	def __init__(self, obj=None):
		Base.__init__(self)
		self._impl = {
			'diff_percentage': '', # Difference percentage
			'page_num': '' , # page number
			'page': '', # page_id
			'hash': '', # document hash
			'version': '', #version
		}
		if obj:
			assert isinstance(obj, dict)
			self.set('page_num', obj['page_num'])
			self.set('diff_percentage', obj['diff_percentage'])
			self.set('page', obj['page'])

	def bson(self, collections):
		page = self.get('page')
		id = None
		if isinstance(page, Base):
			id = page.bson(collections)

		found = collections['difference_metrics'].find_one({'version': self.get('version'), 'hash': self.get('hash'), 'page_num': self.get('page_num')})
		obj = self.__dummy_copy()
		obj['page'] = id
		if found:
			collections['difference_metrics'].update_one({'version': self.get('version'), 'hash': self.get('hash'), 'page_num': self.get('page_num')}, {'page': id})
			return found['_id']
		else:
			inserted = collections['difference_metrics'].insert_one(obj)
			return inserted.inserted_id
			
	def __dummy_copy(self):
		diff = DifferenceMetric()
		ret = diff.obj()
		ret['diff_percentage'] = self.get('diff_percentage')
		ret['page_num'] = self.get('page_num')
		ret['hash'] = self.get('hash')
		ret['version'] = self.get('version')
		return ret



class JsonEncoder(json.JSONEncoder):
	def default(self, o):
		return o.obj()