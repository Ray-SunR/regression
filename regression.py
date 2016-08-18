__author__ = 'Renchen'

import multiprocessing
import documents
import pymongo
from multiprocessing.dummy import Pool as ThreadPool
from PIL import ImageChops
from PIL import Image, ImageEnhance
import os.path
import sys
import subprocess
import json
import re


class Regression(object):
	def __init__(self, src_testdir, ref_outdir, tar_outdir, diff_outdir, concur=4, ref_use_sdk=False,  tar_use_sdk=False,	 ref_bin_dir=None,	tar_bin_dir=None):

		self.__ref_out = ref_outdir
		self.__tar_out = tar_outdir
		self.__diff_out = diff_outdir

		self.__src_testdir = src_testdir

		if not os.path.exists(self.__ref_out):
			os.makedirs(self.__ref_out)

		if not os.path.exists(self.__tar_out):
			os.makedirs(self.__tar_out)

		if not os.path.exists(self.__diff_out):
			os.makedirs(self.__diff_out)

		if not os.path.exists(self.__diff_out):
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
		self.__diff_metrics = {}

		self.__ref_out_paths = []
		self.__tar_out_paths = []
		self.__diff_out_paths = []
		self.__src_file_paths = []

		self.__ref_out_diff_paths_map = {} # map between ref out paths and diff paths
		self.__tar_out_diff_paths_map = {} # map between tar out paths and diff paths

		self.__ref_version = ''
		self.__tar_version = ''

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

	def DiffMetrics(self):
		return self.__diff_metrics

	def __ImageCompare(self, im1, im2):
		try:
			diffcount = 0.0

			imgcompdiff = ImageChops.difference (im1, im2)
			diffboundrect = imgcompdiff.getbbox ()
			imgdiffcrop = imgcompdiff.crop (diffboundrect)

			data = imgdiffcrop.getdata()

			seq = []
			for row in data:
				seq += list(row)

			for i in xrange (0, imgdiffcrop.size[0] * imgdiffcrop.size[1] * 3, 3):
				if seq[i] != 0 or seq[i+1] != 0 or seq[i+2] != 0:
					diffcount = diffcount + 1.0

			diffImgLen = imgcompdiff.size[0] * imgcompdiff.size[1] * 1.0
			diffpercent = (diffcount * 100) / diffImgLen
			return diffpercent
		except IOError:
			#raise Exception, 'Input file does not exist'
			pass

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
		path1, path2, outpath = tuple

		try:
			# The first path is the reference path
			assert os.path.exists(path1)

			if not os.path.exists(path2):
				print(path2 + " doesn't exist! Skipped!")
				return
			else:
				im1 = Image.open(path1)
				img2 = Image.open(path2)
				enhancer = ImageEnhance.Sharpness(img2)
				img2 = enhancer.enhance(8)
				diff_img = ImageChops.difference(im1, img2)
				diff_image_path = os.path.join(outpath, os.path.basename(path1))

				self.__ref_out_diff_paths_map[os.path.basename(path1)] = diff_image_path
				self.__tar_out_diff_paths_map[os.path.basename(path2)] = diff_image_path

				diff_img.save(diff_image_path, 'PNG')
				diff_metrics = documents.DifferenceMetric()
				diff_metrics.set('diff_percentage', self.__ImageCompare(im1, img2))
				self.__diff_metrics[os.path.basename(path1)] = diff_metrics
		except Exception as e:
			print(e)

	def __populate_file_paths(self):
		if not self.__ref_out_paths:
			self.__GetFilesRecursively(self.__ref_out, '*.png', self.__ref_out_paths)

		if not self.__tar_out_paths:
			self.__GetFilesRecursively(self.__tar_out, '*.png', self.__tar_out_paths)

		if not self.__diff_out_paths:
			self.__GetFilesRecursively(self.__diff_out, '*.png', self.__diff_out_paths)

		if not self.__src_file_paths:
			self.__GetFilesRecursively(self.__src_testdir, '*.pdf', self.__src_file_paths)


	def RunImageDiff(self):
		self.__populate_file_paths()
		pool = ThreadPool(8)
		args = []
		for file in self.__ref_out_paths:
			tar_file = os.path.join(self.__tar_out, os.path.relpath(file, self.__ref_out))
			args.append((file, tar_file, self.__diff_out))

		pool.map(self.__RunImageDiffImpl, args)
		pool.close()
		pool.join()

		dict = {}
		dict['diff_metrics'] = self.__diff_metrics
		dict['ref_out_diff_map'] = self.__ref_out_diff_paths_map
		dict['tar_out_diff_map'] = self.__tar_out_diff_paths_map
		with open('cache.json', 'w') as file:
			json_str = json.dumps(dict, ensure_ascii=False, cls=documents.JsonEncoder)
			file.write(json_str)


	def GetAllFiles(self, dir, ext):
		if not os.path.exists(os.path.join(self.__ref_out, os.path.basename(self.__src_testdir))):
			os.makedirs(os.path.join(self.__ref_out, os.path.basename(self.__src_testdir)))

		if not os.path.exists(os.path.join(self.__tar_out, os.path.basename(self.__src_testdir))):
			os.makedirs(os.path.join(self.__tar_out, os.path.basename(self.__src_testdir)))

		if not self.__src_file_paths:
			ret = []
			filenames = os.listdir(dir)
			for filename in filenames:
				if os.path.isdir(os.path.join(dir, filename)):
					rel_path = os.path.relpath(os.path.join(dir, filename), self.__src_testdir)
					basename = os.path.basename(self.__src_testdir)
					# Create output dirs if necessary
					ref_base = os.path.join(self.__ref_out, basename)
					tar_base = os.path.join(self.__tar_out, basename)
					if not os.path.exists(os.path.join(ref_base, rel_path)):
						os.makedirs(os.path.join(ref_base, rel_path))
					if not os.path.exists(os.path.join(tar_base, rel_path)):
						os.makedirs(os.path.join(tar_base, rel_path))
					# Go recursively
					ret += self.GetAllFiles(os.path.join(dir, filename), ext)
				elif os.path.isfile(os.path.join(dir, filename)) and os.path.splitext(filename)[1] == ext:
					ret.append(os.path.join(dir, filename))
			self.__src_file_paths = ret
			return ret
		else:
			return self.__src_file_paths

	def RunAllFiles(self):
		allfiles = self.GetAllFiles(self.__src_testdir, '.pdf')

		allfiles = '|'.join(map(str, allfiles))
		refargs = ['python', 'reg_helper.py','--files', allfiles, '--src_dir', self.__src_testdir, '--ref_out_dir', self.__ref_out, '--concurency', str(self.__concurency), '--use_ref_sdk', str(self.__use_ref_sdk), '--ref_bin_path', self.__ref_bin_dir]

		tarargs = ['python', 'reg_helper.py', '--files', allfiles, '--src_dir', self.__src_testdir, '--tar_out_dir', self.__tar_out, '--concurency', str(self.__concurency), '--use_tar_sdk', str(self.__use_tar_sdk), '--tar_bin_path', self.__tar_bin_dir]

		refregression = subprocess.Popen(refargs)
		tarregression = subprocess.Popen(tarargs)
		refregression.communicate()
		tarregression.communicate()

		self.__populate_file_paths()


	def GetVersion(self):
		refargs = ['python', 'reg_helper.py', '--use_ref_sdk', str(self.__use_ref_sdk), '--version', '--ref_bin_path', self.__ref_bin_dir]
		tarargs = ['python', 'reg_helper.py', '--use_tar_sdk', str(self.__use_tar_sdk), '--version', '--tar_bin_path', self.__tar_bin_dirA]

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
			return 'pdf2image'
		else:
			assert False

	def GetTarType(self):
		if self.__use_tar_sdk:
			return 'sdk'
		elif self.__tar_bin_dir:
			return 'pdf2image'
		else:
			assert False

	def __GetFilesRecursively(self, dir, pattern, ret):
		import fnmatch
		for root, dirnames, filenames in os.walk(dir):
			for filename in fnmatch.filter(filenames, pattern):
				ret.append(os.path.join(root, filename))

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

		if not self.__diff_metrics or not self.__ref_out_diff_paths_map or not self.__tar_out_diff_paths_map:
			with open('cache.json', 'r') as file:
				dict = json.loads(file.read())
				diff_metrics = dict['diff_metrics']
				if not self.__diff_metrics:
					for key in diff_metrics.keys():
						self.__diff_metrics[key] = documents.DifferenceMetric(diff_metrics[key])

				if not self.__ref_out_diff_paths_map:
					self.__ref_out_diff_paths_map = dict['ref_out_diff_map']

				if not self.__tar_out_diff_paths_map:
					self.__tar_out_diff_paths_map = dict['tar_out_diff_map']

		self.__populate_file_paths()

		refversion, tarversion = self.GetVersion()

		refversion = '6.6143323'
		tarversion = '6.7043245'
		alldocs = []
		for path in self.__src_file_paths:
			benchmark = documents.Benchmark()
			if db_benchmarks.find_one({'version': refversion}):
				# only append diffs
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
			id = document.bson(collections)
			ret.append(id)

		print(ret)
		#with open('serializeout.json', 'w') as file:
		#	file.write(json.dumps(ret, ensure_ascii=False, indent=4, separators=(',', ': ')))


def main():
	#regression = Regression(src_testdir='/Users/Renchen/Documents/Work/GitHub/regression/test_files', ref_outdir='/Users/Renchen/Documents/Work/GitHub/regression/ref_out', tar_outdir='/Users/Renchen/Documents/Work/GitHub/regression/tar_out', diff_outdir='/Users/Renchen/Documents/Work/GitHub/regression/diff', concur=4, ref_use_sdk=True, tar_use_sdk=True)

	regression = Regression(src_testdir='/Users/Renchen/Documents/Work/GitHub/regression/test_files', ref_outdir='/Users/Renchen/Documents/Work/GitHub/regression/ref_out', tar_outdir='/Users/Renchen/Documents/Work/GitHub/regression/tar_out', diff_outdir='/Users/Renchen/Documents/Work/GitHub/regression/diff', concur=4, ref_bin_dir='/Users/Renchen/Documents/Work/GitHub/regression/ref_bin/pdf2image', tar_bin_dir='/Users/Renchen/Documents/Work/GitHub/regression/tar_bin/pdf2image')

	regression.RunAllFiles()
	#regression.RunImageDiff()
	#regression.UpdateDatabase()

if __name__ == '__main__':
	main()
	sys.exit(0)
