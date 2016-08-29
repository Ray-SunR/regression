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
			elif isinstance(self._impl[key], dict):
				obj = {}
				for k in self._impl[key].keys():
					diff= self._impl[key][k]
					if isinstance(diff, Base):
						tmp_obj = {}
						diff.serialize(tmp_obj)
						obj[k] = tmp_obj
				bson[key] = obj
			else:
				assert False

	def bson(self, collections, refversion, tarversion):
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
			'references': {} # map between ref version to Benchmarks)
		}

	def bson(self, collections, refversion, tarversion):

		if self.get('document_name') == "YuchenLiu's Driver licence.pdf":
			tmp = 1
		document_found = collections['documents'].find_one({'hash': self.get('hash')})
		if document_found:
			for key in self.get('references').keys():
				reference = self.get('references')[key]
				key = key.replace('.', '_')
				if key in document_found['references'].keys():
					# This reference already exists
					ref_found = collections['references'].find_one({'hash': reference.get('hash'), 'version': reference.get('version')})
					assert (ref_found)

					if tarversion in ref_found['diffs'].keys() and False:
						# Don't need to update
						pass
					else:
						# Create a new diff in the db and append it to references obj
						diff = reference.get('diffs')[tarversion]
						diff_id = diff.bson(collections, refversion, tarversion)

						tarversion = tarversion.replace('.', '_')
						new_key = 'diffs.' + tarversion
						collections['references'].update_one({'hash': reference.get('hash'), 'version': reference.get('version')}, {'$set': {new_key: diff_id}})
				else:
					# need to generate a new reference
					ref_id = reference.bson(collections, refversion, tarversion)
					new_key = 'references.' + refversion
					collections['documents'].update_one({'hash': self.get('hash')}, {'$set': {new_key: ref_id}})
				return document_found['_id']
		else:
			# this is a new document
			ref_ids = {}
			dbobj = self.__dummy_copy()
			dbobj['references'] = ref_ids
			for key in self.get('references').keys():
				reference = self.get('references')[key]
				ref_id = reference.bson(collections, refversion, tarversion)
				refversion = refversion.replace('.', '_')
				ref_ids[refversion] = ref_id

			inserted = collections['documents'].insert_one(dbobj)
			return inserted.inserted_id


	def populate(self, d_path):
		filename,ext = os.path.splitext(d_path)
		self.set('document_name', os.path.basename(d_path))
		self.set('ext', ext)
		self.set('path', d_path)
		if not self.get('hash'):
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

class Difference(Base):
	'''
	A difference object serves as a container for Difference_Metrics
	It represents a target run
	'''
	def __init__(self):
		self._impl = {
			'document_name':'',
			'version':'',
			'hash':'',
			'pages':{}, # this entry will not be stored into the database
			'metrics':{} # a map between page number and diff metrics
		}

	def bson(self, collections, refversion, tarversion):
		# update its pages and metrics
		pages_obj = {}
		metrics_obj = {}
		dbobj =  self.__dummy_copy()
		dbobj['pages'] = pages_obj
		dbobj['metrics'] = metrics_obj
		for key in self.get('pages').keys():
			page = self.get('pages')[key]
			page_id = page.bson(collections, refversion, tarversion)
			pages_obj[str(key)] = page_id

		for key in self.get('metrics').keys():
			metric = self.get('metrics')[key]
			metric_id = metric.bson(collections, refversion, tarversion)
			metrics_obj[str(key)] = metric_id

		collections['differences'].update_one({'hash': self.get('hash'), 'version': self.get('version')}, {'$set': dbobj}, upsert=True)

		return collections['differences'].find_one({'hash': self.get('hash'), 'version': self.get('version')})['_id']

	def __dummy_copy(self):
		diff = Difference()
		diff_obj = diff.obj()
		diff_obj['document_name'] = self.get('document_name')
		diff_obj['version'] = self.get('version')
		diff_obj['hash'] = self.get('hash')
		return diff_obj

# A benchmark is a container for reference runs
class Reference(Base):
	'''
	Uniquely identified by version and hash
	'''
	def __init__(self):
		self._impl = {
			'version': '', # PK
			'type': '', # sdk or pdf2image or docpub etc...
			'hash': '', # pk, document hash
			'pages': {}, # a map between page number and page
			'diffs': {} # a map between target version and Difference
		}

	def bson(self, collections, refversion, tarversion):
		found_ref = collections['references'].find_one({'hash': self.get('hash'), 'version': self.get('version')})

		if found_ref:
			# Only update diffs
			for key in self.get('diffs').keys():
				diff = self.get('diffs')[key]
				diff_id = diff.bson(collections, refversion, tarversion)
				tarversion = tarversion.replace('.', '_')
				new_key = 'diffs.' + tarversion
				collections['references'].update_one({'hash': self.get('hash'), 'version': self.get('version')}, {'$set':{new_key: diff_id}})

			return found_ref['_id']
		else:
			# Create new reference
			reference_dbobj = self.__dummy_copy()
			pages_obj = {}
			diffs_obj = {}
			reference_dbobj['pages'] = pages_obj
			reference_dbobj['diffs'] = diffs_obj
			for key in self.get('pages').keys():
				page = self.get('pages')[key]
				page_id = page.bson(collections, refversion, tarversion)
				pages_obj[str(key)] = page_id

			for key in self.get('diffs').keys():
				diff = self.get('diffs')[key]
				diff_id = diff.bson(collections, refversion, tarversion)
				key = key.replace('.', '_')
				diffs_obj[key] = diff_id

			inserted_ret = collections['references'].insert_one(reference_dbobj)
			return inserted_ret.inserted_id


	def __dummy_copy(self):
		bm = Reference()
		ret = bm.obj()
		ret['version'] = self.get('version')
		ret['type'] = self.get('type')
		ret['hash'] = self.get('hash')
		return ret

	def __filter_files(self, dir, pattern):
		result = {} # page num -> path
		for file in os.listdir(dir):
			ret = re.search(pattern, file)
			if not ret:
				continue
			page_num = int(ret.group(1)) if ret.group(1) else 1
			result[page_num] = os.path.join(dir, file)
		return result

	def populate(self, regression, document):
		# remember: the output generated from binary (pdf2image) for single page pdf will not include the page number
		pattern = os.path.splitext(document.get('document_name'))[0] + '(?:\.png|_(\d+).png)'

		dname = document.get('document_name')
		hash = document.get('hash')
		self.set('hash', hash)

		if regression.out_dir():
			# find the image outputs based on hash
			out_dir = regression.out_dir()
			ref_dir = os.path.join(out_dir, hash, regression.ref_dir_name())
			tar_dir = os.path.join(out_dir, hash, regression.tar_dir_name())
			diff_dir = os.path.join(out_dir, hash, regression.diff_dir_name())

			ref_outs = self.__filter_files(ref_dir, pattern)
			tar_outs = self.__filter_files(tar_dir, pattern)
			diff_outs = self.__filter_files(diff_dir, pattern)

			for page_num in ref_outs.keys():
				page = Page()
				self.get('pages')[page_num] = page

				page.set('hash', hash)
				page.set('version', regression.get_reference_version())
				page.set('document_name', dname)
				page.set('page_num', page_num)
				page.set('ext', 'png')
				page.set('path', os.path.abspath(ref_outs[page_num]))
				with open(ref_outs[page_num], 'r') as mfile:
					page.set('binary', Binary(mfile.read()))

			metrics_tar_map = regression.diff_metrics_tar_map()
			difference = Difference()
			difference.set('version', regression.get_target_version())
			difference.set('hash', hash)
			difference.set('document_name', dname)
			self.get('diffs')[regression.get_target_version()] = difference

			for page_num in tar_outs.keys():
				page = Page()
				difference.get('pages')[page_num] = page

				page.set('hash', hash)
				page.set('version', regression.get_target_version())
				page.set('document_name', dname)
				page.set('page_num', page_num)
				page.set('ext', 'png')
				page.set('path', os.path.abspath(diff_outs[page_num]))
				with open(diff_outs[page_num], 'r') as mfile:
					page.set('binary', Binary(mfile.read()))

				assert tar_outs[page_num] in metrics_tar_map
				metrics = metrics_tar_map[tar_outs[page_num]]
				difference.get('metrics')[page_num] = metrics

				metrics.set('tar_version', regression.get_target_version())
				metrics.set('ref_version', regression.get_reference_version())
				metrics.set('hash', hash)
				metrics.set('document_name', dname)
		else:
			for file in regression.ref_out_file_paths():
				ret = re.search(pattern, file)
				if not ret:
					continue

				page_num = int(ret.group(1)) if ret.group(1) else 1
				page = Page()
				self.get('pages').append(page)

				page.set('hash', hash)
				page.set('version', self.get('version'))
				page.set('document_name', dname)
				page.set('page_num', page_num)
				page.set('ext', 'png')
				with open(file, 'r') as mfile:
					page.set('binary', Binary(mfile.read()))
					page.set('path', file)


				metrics = regression.diff_metrics_ref_map()
				assert file in metrics
				metric = metrics[file]
				metric.set('version', regression.get_target_version())
				metric.set('hash', hash)
				metric.set('document_name', dname)

				if regression.get_target_version() in self.get('diffs').keys():
					metric = self.get('diffs')[regression.get_target_version()][0]
				else:
					self.get('diffs')[regression.get_target_version()] = []
					self.get('diffs')[regression.get_target_version()].append(metric)

				diff_page = Page()
				metric.get('pages').append(diff_page)

				diff_page.set('version', regression.get_target_version())
				diff_page.set('document_name', dname)
				diff_page.set('hash', hash)
				diff_page.set('page_num', page_num)
				diff_page.set('ext', 'png')

				assert file in regression.ref_out_diff_map()
				diff_page_path = regression.ref_out_diff_map()[file]
				with open(diff_page_path, 'r') as mfile:
					diff_page.set('binary', Binary(mfile.read()))
					diff_page.set('path', os.path.abspath(diff_page_path))


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

	def bson(self, collections, refversion, tarversion):
		dbobj = self.__dummy_copy()
		collections['pages'].update_one({'hash': self.get('hash'), 'page_num': self.get('page_num'), 'version': self.get('version')}, {'$set': dbobj}, upsert=True)

		return collections['pages'].find_one({'hash': self.get('hash'), 'page_num': self.get('page_num'), 'version': self.get('version')})['_id']

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
		obj['path'] = self._impl['path']
		#obj['binary'] = self._impl['binary']
		obj['ext'] = self._impl['ext']


class DifferenceMetric(Base):
	'''
	Each metric represents a page that generated by a target
	'''
	def __init__(self, obj=None):
		Base.__init__(self)
		self._impl = {
			'diff_percentage': '', # Difference percentage
			'hash': '', # document hash
			'ref_version': '', #version
			'tar_version': '',
			'document_name': '', # document name
		}
		if obj:
			assert isinstance(obj, dict)
			self.set('diff_percentage', obj['diff_percentage'])
			self.set('document_name', obj['document_name'])

	def bson(self, collections, refversion, tarversion):
		dbobj = self.__dummy_copy()
		collections['difference_metrics'].update_one({'hash': self.get('hash'), 'ref_version': self.get('ref_version'), 'tar_version': self.get('tar_version')}, {'$set': dbobj}, upsert=True)

		return collections['difference_metrics'].find_one({'hash': self.get('hash'), 'ref_version': self.get('ref_version'), 'tar_version': self.get('tar_version')})['_id']

	def __dummy_copy(self):
		diff = DifferenceMetric()
		ret = diff.obj()
		ret['diff_percentage'] = self.get('diff_percentage')
		ret['hash'] = self.get('hash')
		ret['ref_version'] = self.get('ref_version')
		ret['tar_version'] = self.get('tar_version')
		ret['document_name'] = self.get('document_name')
		return ret

class JsonEncoder(json.JSONEncoder):
	def default(self, o):
		return o.obj()