__author__ = 'Renchen'

import documents
import pymongo
from multiprocessing.dummy import Pool as ThreadPool

import os.path
import sys
import subprocess
import json
import re


class Regression(object):
	def __init__(self,
				 src_testdir,
				 ref_outdir=None,
				 tar_outdir=None,
				 diff_outdir=None,
				 out_dir=None, # This parameter overrides the previous three
				 concur=4,
				 ref_use_sdk=False,
				 tar_use_sdk=False,
				 ref_bin_dir=None,
				 tar_bin_dir=None,
				 do_pdf=True,
				 do_docx=False,
				 do_pptx=False,
				 do_diff=False):

		self.__exts = []
		if do_pdf:
			self.__exts.append('.pdf')

		if do_docx:
			self.__exts.append('.docx')

		if do_pptx:
			self.__exts.append('.pptx')

		self.__do_diff = do_diff
		self.__ref_out = ref_outdir
		self.__tar_out = tar_outdir
		self.__diff_out = diff_outdir
		self.__out_dir = None
		if out_dir:
			self.__out_dir = out_dir

			# We are in db mode, all files are centrailized. Under this out_dir,
			# there will be a folder for each document in the src_testdir with
			# their hash name, inside each folder there will be ref, tar, diff
			# folders saving the outputs. So, it's centralized
			self.__ref_out = None
			self.__tar_out = None
			self.__diff_out = None

		self.__src_testdir = src_testdir

		if self.__ref_out and not os.path.exists(self.__ref_out):
			os.makedirs(self.__ref_out)

		if self.__tar_out and not os.path.exists(self.__tar_out):
			os.makedirs(self.__tar_out)

		if self.__ref_out and self.__ref_out == self.__tar_out:
			os.makedirs(os.path.join(self.__ref_out, 'ref'))
			os.makedirs(os.path.join(self.__tar_out, 'tar'))

		if self.__diff_out and not os.path.exists(self.__diff_out):
			os.makedirs(self.__diff_out)

		assert os.path.exists(self.__src_testdir)

		self.__concurency = concur
		self.__use_ref_sdk = ref_use_sdk
		self.__ref_bin_dir = ref_bin_dir if ref_bin_dir else ''
		self.__use_tar_sdk = tar_use_sdk
		self.__tar_bin_dir = tar_bin_dir if tar_bin_dir else ''
		#assert self.__use_ref_sdk and not self.__ref_bin_dir or self.__ref_bin_dir and not self.__use_ref_sdk
		#assert self.__use_tar_sdk and not self.__tar_bin_dir or self.__tar_bin_dir and not self.__use_tar_sdk

		self.__documents = []
		self.__benchmarks = []
		self.__pages = []
		self.__differences = []

		# map from ref path to diff metrics
		self.__diff_metrics_ref_map = {}

		# map from tar path to diff metrics
		self.__diff_metrics_tar_map = {}

		# map between ref out path to diff path
		self.__ref_out_diff_paths_map = {}

		# map between tar out path to diff path
		self.__tar_out_diff_paths_map = {}

		# map between src file path to hash code
		self.__src_path_hashmap = {}

		self.__ref_out_paths = []
		self.__tar_out_paths = []
		self.__diff_out_paths = []
		self.__src_file_paths = []

		self.__ref_version, self.__tar_version = self.get_versions()

		# Set maximum number of pages to run image diff
		self.__num_pages_to_diff = 10


	# Retun the relationship between ref out files and diff files
	# store their paths
	def ref_out_diff_map(self):
		return self.__ref_out_diff_paths_map

	def out_dir(self):
		return self.__out_dir

	def ref_out(self):
		return self.__ref_out

	def tar_out(self):
		return self.__tar_out

	def ref_out_file_paths(self):
		return self.__ref_out_paths

	def tar_out_file_paths(self):
		return self.__tar_out_paths

	def diff_out_file_paths(self):
		return self.__diff_out_paths

	def src_file_paths(self):
		return self.__src_file_paths

	def diff_metrics_ref_map(self):
		return self.__diff_metrics_ref_map

	def diff_metrics_tar_map(self):
		return self.__diff_metrics_tar_map

	def __run_image_diff_impl(self, tuple):
		args = [sys.executable, 'image_diff.py', '--file1', tuple[0], '--file2', tuple[1], '--output', tuple[2]]
		try:
			print('Running diff for %s and %s' % (tuple[0], tuple[1]))
		except:
			pass
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		stdout = process.communicate()[0].decode('utf-8')
		if stdout:
			try:
				retdict = json.loads(stdout)
				diff_image_path = retdict['diff_image_path']
				diff_percentage = retdict['diff_percentage']
				diff_metrics = documents.DifferenceMetric()
				diff_metrics.set('diff_percentage', diff_percentage)

				self.__ref_out_diff_paths_map[tuple[0]] = diff_image_path
				self.__tar_out_diff_paths_map[tuple[1]] = diff_image_path
				self.__diff_metrics_ref_map[tuple[0]] = diff_metrics
				self.__diff_metrics_tar_map[tuple[1]] = diff_metrics
			except Exception as e:
				print(e)

	def __populate_file_paths(self):
		if not self.__src_file_paths:
				self.__get_files_recursively(self.__src_testdir, self.__exts, self.__src_file_paths)

		if self.__out_dir:
			# if this is a centralize mode, then we don't need to populate ref_out, tar_out, diff_out, because the outputs reside in each document's hash
			pass
		else:
			if not self.__ref_out_paths:
				self.__get_files_recursively(self.__ref_out, ['.png'], self.__ref_out_paths)

			if not self.__tar_out_paths:
				self.__get_files_recursively(self.__tar_out, ['.png'], self.__tar_out_paths)

			if not self.__diff_out_paths:
				self.__get_files_recursively(self.__diff_out, ['.png'], self.__diff_out_paths)

	def __delete_all(self, folder):
		import os, shutil
		try:
			for the_file in os.listdir(folder):
				file_path = os.path.join(folder, the_file)
				try:
					if os.path.isfile(file_path):
						os.unlink(file_path)
					elif os.path.isdir(file_path):
						shutil.rmtree(file_path)
				except Exception as e:
					print(e)
		except Exception as e:
			print(e)

	def __cache(self):
		dict = {}
		dict['out_dir'] = '' if self.__out_dir else self.__out_dir
		dict['ref_out'] = '' if self.__ref_out else self.__ref_out
		dict['tar_out'] = '' if self.__tar_out else self.__tar_out
		dict['ref_bin'] = self.__ref_bin_dir
		dict['tar_bin'] = self.__tar_bin_dir
		dict['diff_metrics_ref_map'] = self.__diff_metrics_ref_map
		dict['diff_metrics_tar_map'] = self.__diff_metrics_tar_map
		dict['ref_out_diff_map'] = self.__ref_out_diff_paths_map
		dict['tar_out_diff_map'] = self.__tar_out_diff_paths_map
		with open('cache.json', 'wb') as file:
			json_str = json.dumps(dict, ensure_ascii=False, cls=documents.JsonEncoder)
			file.write(json_str.encode('utf-8'))

	def __filter_files(self, dir, pattern):
		result = {} # page num -> path
		for file in os.listdir(dir):
			ret = re.search(pattern, file)
			if not ret:
				continue
			page_num = int(ret.group(1)) if ret.group(1) else 1
			result[page_num] = os.path.join(dir, file)
		return result

	def run_image_diff(self):
		self.__populate_file_paths()
		pool = ThreadPool(self.__concurency)
		args = []

		if self.__out_dir:
			for file in self.__src_file_paths:
				pattern = re.escape(os.path.splitext(os.path.basename(file))[0]) + '(?:\.png|_(\d+).png)'
				try:
					hash = self.__hash(file)
					if not hash:
						continue
					ref_image_paths = []
					tar_image_paths = []
					ref_outs = self.__filter_files(os.path.join(self.__out_dir, hash, 'ref', self.__ref_version), pattern)
					tar_outs = self.__filter_files(os.path.join(self.__out_dir, hash, 'tar'), pattern)

					ref_image_names = []
					tar_image_names = []
					for key in ref_outs.keys():
						if key > self.__num_pages_to_diff:
							continue
						ref_image_names.append(os.path.split(ref_outs[key])[1])
						ref_image_paths.append(ref_outs[key])
						if len(ref_image_paths) == self.__num_pages_to_diff:
							break

					for key in tar_outs.keys():
						if key > self.__num_pages_to_diff:
							continue
						tar_image_names.append(os.path.split(tar_outs[key])[1])
						tar_image_paths.append(tar_outs[key])
						if len(tar_image_paths) == self.__num_pages_to_diff:
							break

					folder_name = self.__ref_version + '-' + self.__tar_version
					diffpath = os.path.join(self.__out_dir, hash, 'diff', folder_name)
					if os.path.exists(diffpath):
						self.__delete_all(diffpath)
					else:
						os.makedirs(diffpath)

					for image_path in ref_image_paths:
						if os.path.split(image_path)[1] in tar_image_names:
							import time
							args.append((image_path, os.path.join(self.__out_dir, hash, 'tar', os.path.split(image_path)[1]), diffpath))
						else:
							tl=False
				except Exception as e:
					print(e)
		else:
			for file in self.__ref_out_paths:
				tar_file = os.path.join(self.__tar_out, os.path.relpath(file, self.__ref_out))
				args.append((file, tar_file, self.__diff_out))

		pool.map(self.__run_image_diff_impl, args)
		pool.close()
		pool.join()

		self.__cache()


	def __get_all_files(self, dir, ext_list):
		if not self.__src_file_paths:
			for root, subFolders, files in os.walk(dir):
				for file in files:
					try:
						if os.path.splitext(file)[1].lower() in ext_list:
							if not self.__out_dir:
								relpath = os.path.relpath(root, self.__src_testdir)
								if self.__ref_out:
									path = os.path.join(self.__ref_out, os.path.basename(self.__src_testdir), relpath)
									if not os.path.exists(path):
										os.makedirs(path)
									else:
										self.__delete_all(path)

								if self.__tar_out:
									path = os.path.join(self.__tar_out, os.path.basename(self.__src_testdir), relpath)
									if not os.path.exists(path):
										os.makedirs(path)
									else:
										self.__delete_all(path)
							try:
								print(os.path.join(root, file) + ' added to queue')
							except:
								pass
							self.__src_file_paths.append(os.path.join(root, file))
					except Exception as e:
						print(e)
		return self.__src_file_paths

	def __run_all_files_impl(self, files):
		if not files:
			return
		if not isinstance(files, list):
			files = [files]
		allfiles = '|'.join(files)
		import hashlib
		hash = hashlib.sha1(allfiles.encode('utf-8')).hexdigest()
		allfiles_fn = os.path.split(self.__src_testdir)[1] + '_' + hash + '.txt'
		with open(allfiles_fn, 'wb') as file:
			file.write(allfiles.encode('utf-8'))
		refargs = [sys.executable, 'reg_helper.py',
				   '--filename', allfiles_fn,
				   '--src_dir', self.__src_testdir,
				   '--ref_out_dir', '' if not self.__ref_out else self.__ref_out,
				   '--concurency', str(self.__concurency),
				   '--use_ref_sdk', str(self.__use_ref_sdk),
				   '--ref_bin_path', self.__ref_bin_dir,
				   '--ref_version_name', self.__ref_version,
				   '--out_dir', '' if not self.__out_dir else self.__out_dir]

		tarargs = [sys.executable, 'reg_helper.py',
				   '--filename', allfiles_fn,
				   '--src_dir', self.__src_testdir,
				   '--tar_out_dir', '' if not self.__tar_out else self.__tar_out,
				   '--concurency', str(self.__concurency),
				   '--use_tar_sdk', str(self.__use_tar_sdk),
				   '--tar_bin_path', self.__tar_bin_dir,
				   '--out_dir', '' if not self.__out_dir else self.__out_dir]

		if (self.__ref_out or self.__out_dir) and (self.__use_ref_sdk or self.__ref_bin_dir):
			refregression = subprocess.Popen(refargs)
			refregression.communicate()

		if (self.__tar_out or self.__out_dir) and (self.__use_tar_sdk or self.__tar_bin_dir) :
			tarregression = subprocess.Popen(tarargs)
			tarregression.communicate()

		self.__populate_file_paths()

		if self.__do_diff:
			self.run_image_diff()
		else:
			self.__cache()

	def run_all_files(self):
		allfiles = self.__get_all_files(self.__src_testdir, self.__exts)
		self.__run_all_files_impl(allfiles)

	def ref_dir_name(self):
		return os.path.join('ref', self.__ref_version) if self.__out_dir else self.__ref_out

	def tar_dir_name(self):
		return 'tar' if self.__out_dir else self.__tar_out

	def diff_dir_name(self):
		return os.path.join('diff', self.__ref_version + '-' + self.__tar_version) if self.__out_dir else self.__diff_out

	def get_versions(self):
		refargs = [sys.executable, 'reg_helper.py', '--use_ref_sdk', '' if not self.__use_ref_sdk else self.__use_ref_sdk, '--version', '--ref_bin_path', self.__ref_bin_dir]
		tarargs = [sys.executable, 'reg_helper.py', '--use_tar_sdk', '' if not self.__use_tar_sdk else self.__use_tar_sdk, '--version', '--tar_bin_path', self.__tar_bin_dir]

		pattern = b'.*(\d+\.\d+).'

		refstdout = ''
		tarstdout = ''
		if self.__use_ref_sdk or self.__ref_bin_dir:
			refversion = subprocess.Popen(refargs, stdout=subprocess.PIPE)
			refstdout = refversion.communicate()[0]
			ret = re.search(pattern, refstdout)
			if ret:
				refstdout = ret.group(1)

		if self.__use_tar_sdk or self.__tar_bin_dir:
			tarversion = subprocess.Popen(tarargs, stdout=subprocess.PIPE)
			tarstdout = tarversion.communicate()[0]
			ret = re.search(pattern, tarstdout)
			if ret:
				tarstdout = ret.group(1)

		self.__ref_version = refstdout.decode('utf-8')
		self.__tar_version = tarstdout.decode('utf-8')

		# Mongodb doens't like keys with '.' inside
		self.__ref_version = self.__ref_version.replace('.', '_')
		self.__tar_version = self.__tar_version.replace('.', '_')
		return (self.__ref_version , self.__tar_version )

	def get_reference_version(self):
		return self.__ref_version

	def get_target_version(self):
		return self.__tar_version

	def get_reference_run_type(self):
		if self.__use_ref_sdk:
			return 'sdk'
		elif self.__ref_bin_dir:
			return os.path.splitext(os.path.split(self.__ref_bin_dir)[1])[0]
		else:
			''

	def get_target_run_type(self):
		if self.__use_tar_sdk:
			return 'sdk'
		elif self.__tar_bin_dir:
			return os.path.splitext(os.path.split(self.__tar_bin_dir)[1])[0]
		else:
			''

	def __get_files_recursively(self, dir, exts, ret):
		if not dir:
			return
		try:
			for root, dirnames, filenames in os.walk(dir):
				for filename in filenames:
					if os.path.splitext(filename)[1] in exts:
						ret.append(os.path.join(root, filename))
		except Exception as e:
			print (e)

	def __hash(self, filepath):
		import hashlib
		try:
			with open(filepath, 'rb') as file:
				sha1 = hashlib.sha1(file.read())
				# Append file name in order to avoid conflicts
				return sha1.hexdigest() + '_' + os.path.split(filepath)[1]
		except Exception as e:
			print(e)

	def __collections(self):
		client = pymongo.MongoClient()
		#client.drop_database('pdftron_regression')
		db = client.pdftron_regression

		db_documents = db.documents
		db_references = db.references
		db_pages = db.pages
		db_differences = db.differences
		db_difference_metrics = db.difference_metrics
		return {
			'documents': db_documents,
			'pages': db_pages,
			'differences': db_differences,
			'references': db_references,
			'difference_metrics': db_difference_metrics
		}

	def __recover_cache(self):
		if not self.__diff_metrics_ref_map or not self.__diff_metrics_tar_map or not self.__ref_out_diff_paths_map or not self.__tar_out_diff_paths_map:
			with open('cache.json', 'rb') as file:
				dict = json.loads(file.read().decode('utf-8'))
				diff_metrics_ref_map = dict['diff_metrics_ref_map']
				diff_metrics_tar_map = dict['diff_metrics_tar_map']

				if not self.__diff_metrics_ref_map:
					for key in diff_metrics_ref_map.keys():
						self.__diff_metrics_ref_map[key] = documents.DifferenceMetric(diff_metrics_ref_map[key])

				if not self.__diff_metrics_tar_map:
					for key in diff_metrics_tar_map.keys():
						self.__diff_metrics_tar_map[key] = documents.DifferenceMetric(diff_metrics_tar_map[key])

				if not self.__ref_out_diff_paths_map:
					self.__ref_out_diff_paths_map = dict['ref_out_diff_map']

				if not self.__tar_out_diff_paths_map:
					self.__tar_out_diff_paths_map = dict['tar_out_diff_map']

				if not self.__out_dir:
					self.__out_dir = dict['out_dir']

				if not self.__ref_out:
					self.__ref_out = dict['ref_out']

				if not self.__tar_out:
					self.__tar_out = dict['tar_out']

				if not self.__ref_bin_dir:
					self.__ref_bin_dir = dict['ref_bin']

				if not self.__tar_bin_dir:
					self.__tar_bin_dir = dict['tar_bin']


		self.__populate_file_paths()

	def __get_document_tags(self, dpath):
		lst = [dpath, self.__src_testdir]
		prefix = os.path.commonprefix(lst)
		relpath = os.path.relpath(dpath, prefix)
		relpath_nofname = os.path.split(relpath)[0]
		# using this relpath_nofname to get tags
		ret = [os.path.split(self.__src_testdir)[1]]
		while relpath_nofname:
			(relpath_nofname, tag) = os.path.split(relpath_nofname)
			ret.append(tag)
		return ret

	def __serialize_impl(self, args):
		document = args[0]
		container = args[1]
		obj = {}
		document.serialize(obj)
		container.append(obj)

	def __dumpto_database_impl(self, args):
		document = args[0]
		collections = args[2]
		document.bson(collections, self.__ref_version, self.__tar_version)
		try:
			print(document.get('document_name') + ' dumped to database successfully')
		except Exception as e:
			print(e)

	def update_database(self):
		# Only possible through centralized mode
		if not self.__out_dir:
			return

		collections = self.__collections()
		self.__recover_cache()
		refversion, tarversion = self.get_versions()

		alldocs = []
		for path in self.__src_file_paths:
			try:
				hash = self.__hash(path)
				if not hash:
					continue
				benchmark = documents.Reference()
				found_benchmark = collections['references'].find_one({'version': refversion, 'hash': hash})
				if found_benchmark:
					pass
				else:
					# brand new
					benchmark.set('type', self.get_reference_run_type())
					benchmark.set('version', refversion)

				tags = self.__get_document_tags(path)

				document = documents.Document()
				document.get('references')[refversion] = benchmark
				document.set('hash', hash)
				document.set('tags', tags)
				document.populate(path)

				benchmark.set('version', refversion)
				benchmark.populate(self, document)
				alldocs.append(document)
			except Exception as e:
				print(e)

		serialize_ret = []
		args = [(document, serialize_ret, collections) for document in alldocs]

		pool = ThreadPool(self.__concurency)
		pool.map(self.__serialize_impl, args)
		pool.close()
		pool.join()

		pool = ThreadPool(self.__concurency)
		pool.map(self.__dumpto_database_impl, args)
		pool.close()
		pool.join()

		with open('serializeout.json', 'wb') as file:
			file.write(json.dumps(serialize_ret, ensure_ascii=False, indent=4, separators=(',', ': ')).encode('utf-8'))

	def run(self):
		self.run_all_files()
		self.update_database()

def main():

	# Simple regression mode
	#regression = Regression(src_testdir='test_files', ref_outdir='test_out/ref', tar_outdir='test_out/tar', diff_outdir='test_out/diff', ref_bin_dir='ref_bin/pdf2image', tar_bin_dir='tar_bin/pdf2image', do_pdf=True, do_diff=True)

	# Centralized mode

	import time
	start_time = time.time()
	regression = Regression(src_testdir='test_files/', out_dir='test_out', concur=8,tar_bin_dir='tar_bin/pdf2image' ,ref_bin_dir='ref_bin/pdf2image', do_diff=True)
	regression.run()
	#regression.update_database()

	#regression = Regression(src_testdir='D:/PDFTest/Miscellaneous', out_dir='G:/Regression', concur=8, tar_bin_dir='tar_bin/docpub.exe', ref_bin_dir='ref_bin/docpub.exe', do_diff=True)

	#regression.run_all_files()
	#regression.run_image_diff()
	#regression.update_database()
	print('Elapsed: ' + str(time.time() - start_time))

if __name__ == '__main__':
	main()
	sys.exit(0)
