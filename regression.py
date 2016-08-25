__author__ = 'Renchen'

import multiprocessing
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
		assert self.__use_ref_sdk and not self.__ref_bin_dir or self.__ref_bin_dir and not self.__use_ref_sdk
		assert self.__use_tar_sdk and not self.__tar_bin_dir or self.__tar_bin_dir and not self.__use_tar_sdk

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

		self.__ref_version, self.__tar_version = self.GetVersion()

	# Retun the relationship between ref out files and diff files
	# store their paths
	def RefOutDiffMap(self):
		return self.__ref_out_diff_paths_map

	def RefOutFilePaths(self):
		return self.__ref_out_paths

	def TarOutFilePaths(self):
		return self.__tar_out_paths

	def DiffOutFilePaths(self):
		return self.__diff_out_paths

	def SrcFilePaths(self):
		return self.__src_file_paths

	def DiffMetricsRefMap(self):
		return self.__diff_metrics_ref_map

	def DiffMetricsTarMap(self):
		return self.__diff_metrics_tar_map

	def __RunRegressionImpl(self, lib, filepath, output_path):
		try:
			wordoc = lib.PDFDoc(filepath)
			draw = lib.PDFDraw()
			draw.SetDPI(92)

			it = wordoc.GetPageIterator()
			pagenum = 1
			prefix = os.path.commonprefix([filepath, self.__src_testdir])
			tail = os.path.relpath(filepath, prefix)
			basename = os.path.basename(self.__src_testdir)
			while (it.HasNext()):
				output_file = os.path.join(output_path, basename, tail + "_" + str(pagenum) + ".png")
				draw.Export(it.Current(), output_file)
				sys.stdout.flush()
				print output_file
				it.Next()
				pagenum += 1
		except Exception as e:
			print e

	def __RunImageDiffImpl(self, tuple):
		args = ['python', 'image_diff.py', '--file1', tuple[0], '--file2', tuple[1], '--output', tuple[2]]
		print('Running diff for %s and %s' % (tuple[0], tuple[1]))
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		stdout = process.communicate()[0]
		if stdout:
			retdict = json.loads(stdout.replace("\'", '"'))
			diff_image_path = retdict['diff_image_path']
			diff_percentage = retdict['diff_percentage']
			diff_metrics = documents.DifferenceMetric()
			diff_metrics.set('diff_percentage', diff_percentage)

			self.__ref_out_diff_paths_map[tuple[0]] = diff_image_path
			self.__tar_out_diff_paths_map[tuple[1]] = diff_image_path
			self.__diff_metrics_ref_map[tuple[0]] = diff_metrics
			self.__diff_metrics_tar_map[tuple[1]] = diff_metrics

	def __populate_file_paths(self):
		if not self.__src_file_paths:
				self.__GetFilesRecursively(self.__src_testdir, self.__exts, self.__src_file_paths)

		if self.__out_dir:
			# if this is a centralize mode, then we don't need to populate ref_out, tar_out, diff_out, because the outputs reside in each document's hash
			pass
		else:
			if not self.__ref_out_paths:
				self.__GetFilesRecursively(self.__ref_out, ['.png'], self.__ref_out_paths)

			if not self.__tar_out_paths:
				self.__GetFilesRecursively(self.__tar_out, ['.png'], self.__tar_out_paths)

			if not self.__diff_out_paths:
				self.__GetFilesRecursively(self.__diff_out, ['.png'], self.__diff_out_paths)

	def __delete_all(self, folder):
		import os, shutil
		for the_file in os.listdir(folder):
			file_path = os.path.join(folder, the_file)
			try:
				if os.path.isfile(file_path):
					os.unlink(file_path)
				elif os.path.isdir(file_path):
					shutil.rmtree(file_path)
			except Exception as e:
				print(e)

	def RunImageDiff(self):
		self.__populate_file_paths()
		pool = ThreadPool(8)
		args = []

		if self.__out_dir:
			for file in self.__src_file_paths:
				hash = self.__hash(file)
				ref_image_paths = []
				tar_image_paths = []
				self.__GetFilesRecursively(os.path.join(self.__out_dir, hash, 'ref'), '.png', ref_image_paths)
				self.__GetFilesRecursively(os.path.join(self.__out_dir, hash, 'tar'), '.png', tar_image_paths)
				ref_image_names = [os.path.split(path)[1] for path in ref_image_paths]
				tar_image_names = [os.path.split(path)[1] for path in tar_image_paths]
				for image_path in ref_image_paths:
					if os.path.split(image_path)[1] in tar_image_names:
						diffpath = os.path.join(self.__out_dir, hash, 'diff')
						if os.path.exists(diffpath):
							self.__delete_all(diffpath)
						else:
							os.makedirs(diffpath)
						args.append((image_path, os.path.join(self.__out_dir, hash, 'tar', os.path.split(image_path)[1]), diffpath))
					else:
						print('Missing page ' + image_path + ' for doc: ' + file + ' for target revision: ' + self.__tar_version)
		else:
			for file in self.__ref_out_paths:
				tar_file = os.path.join(self.__tar_out, os.path.relpath(file, self.__ref_out))
				args.append((file, tar_file, self.__diff_out))

		pool.map(self.__RunImageDiffImpl, args)
		pool.close()
		pool.join()

		dict = {}
		dict['diff_metrics'] = self.__diff_metrics_ref_map
		dict['ref_out_diff_map'] = self.__ref_out_diff_paths_map
		dict['tar_out_diff_map'] = self.__tar_out_diff_paths_map
		with open('cache.json', 'w') as file:
			json_str = json.dumps(dict, ensure_ascii=False, cls=documents.JsonEncoder)
			file.write(json_str)


	def GetAllFiles(self, dir, ext_list):
		if not self.__src_file_paths:
			for root, subFolders, files in os.walk(dir):
				for file in files:
					if os.path.splitext(file)[1] in ext_list:
						if not self.__out_dir:
							relpath = os.path.relpath(root, self.__src_testdir)
							path = os.path.join(self.__ref_out, os.path.basename(self.__src_testdir), relpath)
							if not os.path.exists(path):
								os.makedirs(path)
							else:
								self.__delete_all(path)

							path = os.path.join(self.__tar_out, os.path.basename(self.__src_testdir), relpath)
							if not os.path.exists(path):
								os.makedirs(path)
							else:
								self.__delete_all(path)

						print(os.path.join(root, file) + ' added to queue')
						self.__src_file_paths.append(os.path.join(root, file))

		return self.__src_file_paths

	def RunAllFiles(self):
		allfiles = self.GetAllFiles(self.__src_testdir, self.__exts)

		allfiles = '|'.join(map(str, allfiles))
		with open('allfiles.txt', 'w') as file:
			file.write(allfiles)

		refargs = ['python', 'reg_helper.py',
				   '--files', '',
				   '--src_dir', self.__src_testdir,
				   '--ref_out_dir', '' if not self.__ref_out else self.__ref_out,
				   '--concurency', str(self.__concurency),
				   '--use_ref_sdk', str(self.__use_ref_sdk),
				   '--ref_bin_path', self.__ref_bin_dir,
				   '--out_dir', '' if not self.__out_dir else self.__out_dir]

		tarargs = ['python', 'reg_helper.py',
				   '--files', '',
				   '--src_dir', self.__src_testdir,
				   '--tar_out_dir', '' if not self.__tar_out else self.__tar_out,
				   '--concurency', str(self.__concurency),
				   '--use_tar_sdk', str(self.__use_tar_sdk),
				   '--tar_bin_path', self.__tar_bin_dir,
				   '--out_dir', '' if not self.__out_dir else self.__out_dir]

		refregression = subprocess.Popen(refargs)
		tarregression = subprocess.Popen(tarargs)
		refregression.communicate()
		tarregression.communicate()

		self.__populate_file_paths()

		if self.__do_diff:
			self.RunImageDiff()


	def GetVersion(self):
		refargs = ['python', 'reg_helper.py', '--use_ref_sdk', str(self.__use_ref_sdk), '--version', '--ref_bin_path', self.__ref_bin_dir]
		tarargs = ['python', 'reg_helper.py', '--use_tar_sdk', str(self.__use_tar_sdk), '--version', '--tar_bin_path', self.__tar_bin_dir]

		refversion = subprocess.Popen(refargs, stdout=subprocess.PIPE)
		tarversion = subprocess.Popen(tarargs, stdout=subprocess.PIPE)

		refstdout = refversion.communicate()[0]
		tarstdout = tarversion.communicate()[0]

		self.__ref_version = refstdout
		self.__tar_version = tarstdout
		return (refstdout, tarstdout)

	def GetRefVersion(self):
		return self.__ref_version

	def GetTarVersion(self):
		return self.__tar_version

	def GetRefType(self):
		if self.__use_ref_sdk:
			return 'sdk'
		elif self.__ref_bin_dir:
			return os.path.split(self.__ref_bin_dir)[1]
		else:
			assert False

	def GetTarType(self):
		if self.__use_tar_sdk:
			return 'sdk'
		elif self.__tar_bin_dir:
			return os.path.split(self.__tar_bin_dir)[1]
		else:
			assert False

	def __GetFilesRecursively(self, dir, exts, ret):
		for root, dirnames, filenames in os.walk(dir):
			for filename in filenames:
				if os.path.splitext(filename)[1] in exts:
					ret.append(os.path.join(root, filename))

	def __hash(self, filepath):
		import hashlib
		with open(filepath, 'r') as file:
			sha1 = hashlib.sha1(file.read())
			return sha1.hexdigest()

	def UpdateDatabase(self):
		client = pymongo.MongoClient()
		client.drop_database('pdftron_regression')
		db = client.pdftron_regression

		db_documents = db.documents
		db_benchmarks = db.benchmarks
		db_runs = db.runs
		db_pages = db.pages
		db_differences = db.differences
		db_difference_metrics = db.difference_metrics
		collections = {
			'documents': db_documents,
			'benchmarks': db_benchmarks,
			'pages': db_pages,
			'differences': db_differences,
			'runs': db_runs,
			'difference_metrics': db_difference_metrics
		}

		if not self.__diff_metrics_ref_map or not self.__ref_out_diff_paths_map or not self.__tar_out_diff_paths_map:
			with open('cache.json', 'r') as file:
				dict = json.loads(file.read())
				diff_metrics = dict['diff_metrics']
				if not self.__diff_metrics_ref_map:
					for key in diff_metrics.keys():
						self.__diff_metrics_ref_map[key] = documents.DifferenceMetric(diff_metrics[key])

				if not self.__ref_out_diff_paths_map:
					self.__ref_out_diff_paths_map = dict['ref_out_diff_map']

				if not self.__tar_out_diff_paths_map:
					self.__tar_out_diff_paths_map = dict['tar_out_diff_map']

		self.__populate_file_paths()

		refversion, tarversion = self.GetVersion()

		alldocs = []
		for path in self.__src_file_paths:
			hash = self.__hash(path)
			benchmark = documents.Benchmark()
			found_benchmark = db_benchmarks.find_one({'version': refversion, 'hash': hash})
			if found_benchmark:

				pass
			else:
				# brand new
				benchmark.set('type', self.GetRefType())
				benchmark.set('version', refversion)

			run = documents.Run()
			benchmark.get('runs').append(run)

			document = documents.Document()
			document.get('benchmarks').append(benchmark)
			document.populate(path)

			run.populate(document)
			run.set('version', refversion)

			benchmark.populate(self, document, run)
			alldocs.append(document)

		ret = []
		for document in alldocs:
			#obj = {}
			#document.serialize(obj)
			#ret.append(obj)
			id = document.bson(collections, self.__ref_version, self.__tar_version)
			ret.append(id)

		print(ret)
		#with open('serializeout.json', 'w') as file:
		#	file.write(json.dumps(ret, ensure_ascii=False, indent=4, separators=(',', ': ')))


def main():
	#regression = Regression(src_testdir='/Users/Renchen/Documents/Work/GitHub/regression/test_files', ref_outdir='/Users/Renchen/Documents/Work/GitHub/regression/ref_out', tar_outdir='/Users/Renchen/Documents/Work/GitHub/regression/tar_out', diff_outdir='/Users/Renchen/Documents/Work/GitHub/regression/diff', concur=4, ref_use_sdk=True, tar_use_sdk=True)

	regression = Regression(src_testdir='D:/OfficeTest/UnitTests',
							ref_outdir='D:/Regression/Ref',
							tar_outdir='D:/Regression/Target',
							diff_outdir='D:/Regression/Diff',
							concur=4,
							ref_bin_dir='D:/Work/Github/regression/ref_bin/docpub.exe',
							tar_bin_dir='D:/Work/Github/regression/tar_bin/docpub.exe',
							do_pdf=False,
							do_docx=True,
							do_pptx=True)

	#regression = Regression(src_testdir='D:/PDFTest/Annotations', out_dir='D:/Regression', concur=4, ref_bin_dir='D:/Work/Github/regression/ref_bin/docpub.exe', tar_bin_dir='D:/Work/Github/regression/tar_bin/docpub.exe')

	regression.RunAllFiles()
	#regression.UpdateDatabase()

if __name__ == '__main__':
	main()
	sys.exit(0)
