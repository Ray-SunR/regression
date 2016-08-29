__author__ = 'Renchen'

from multiprocessing.dummy import Pool as ThreadPool
import os.path
import sys
import argparse
import subprocess
import re

class Regression(object):
	def __init__(self,
				 files,
				 src_dir,
				 version=False,
				 ref_output_dir=None,
				 tar_output_dir=None,
				 out_dir=None,
				 concur=4,
				 ref_use_sdk=False,
				 ref_bin_dir=None,
				 tar_use_sdk=False,
				 tar_bin_dir=None,
				 ref_version_name=None):

		self.__lib = None
		self.__bin_path = None
		if version:
			if ref_use_sdk:
				self.__ImportRefLib()
			elif tar_use_sdk:
				self.__ImportTarLib()
			elif ref_bin_dir:
				self.__bin_path = ref_bin_dir
			elif tar_bin_dir:
				self.__bin_path = tar_bin_dir
			print(str(self.__GetVersion()))
			return

		if isinstance(ref_use_sdk, str):
			ref_use_sdk = ref_use_sdk in ['true', 'True', 'TRUE', '1']
		if isinstance(tar_use_sdk, str):
			tar_use_sdk = tar_use_sdk in ['true', 'True', 'TRUE', '1']
		assert (ref_use_sdk and not ref_bin_dir or ref_bin_dir and not ref_use_sdk) or (tar_use_sdk and not tar_bin_dir or tar_bin_dir and not tar_use_sdk)

		self.__output_dir = None
		self.__centrailize_mode = False
		if out_dir:
			self.__centrailize_mode = True
			self.__output_dir = out_dir
		else:
			assert ref_output_dir and not tar_output_dir or tar_output_dir and not ref_output_dir
			if ref_output_dir:
				self.__output_dir = ref_output_dir
			elif tar_output_dir:
				self.__output_dir = tar_output_dir

		self.__files = files.split('|')
		self.__src_testdir = src_dir

		self.__ref_or_tar = ''
		if ref_use_sdk:
			self.__ImportRefLib()
			self.__ref_or_tar = 'ref'
		elif tar_use_sdk:
			self.__ImportTarLib()
			self.__ref_or_tar = 'tar'

		self.__bin_path = None
		if ref_bin_dir:
			self.__bin_path = ref_bin_dir
			self.__ref_or_tar = 'ref'
			assert ref_version_name

			# This member is used to track whether we should run for this task because
			# references should only be generated once
			self.__ref_version_name = ref_version_name
		elif tar_bin_dir:
			self.__bin_path = tar_bin_dir
			self.__ref_or_tar = 'tar'

		assert self.__ref_or_tar
		assert self.__lib and not self.__bin_path or self.__bin_path and not self.__lib
		assert self.__files and self.__output_dir

		if self.__bin_path:
			assert os.path.exists(self.__bin_path)

		self.__concurency = concur
		self.__license = ""#"Renchen:ENTCPU:1::W+:AMS(20161216):F97CE727551EB1D47956138CE26EE06F06009EF638D64AB231F5C7"

	def __ImportRefLib(self):
		try:
			from reference_lib import PDFNetPython as RefLib
			self.__lib = RefLib
		except ImportError as e:
			print(e)
			print('Lib not found! Please please the reference lib in ./referecen_lib folder!')
			sys.exit(-1)

	def __ImportTarLib(self):
		try:
			from target_lib import PDFNetPython as TarLib
			self.__lib = TarLib
		except ImportError as e:
			print(e)
			print('Lib not found! Please place the target lib in ./target_lib folder!')
			sys.exit(-1)

	def __GetVersion(self):
		if self.__lib:
			return self.__lib.PDFNet.get_versions()
		else:
			#assert self.__bin_path
			if not self.__bin_path:
				return ''
			if os.path.exists(self.__bin_path):
				commands = [self.__bin_path, '-v']
				process = subprocess.Popen(commands, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
				return process.communicate()[0]

	def __hash(self, fpath):
		import hashlib
		with open(fpath, 'r') as file:
			sha1 = hashlib.sha1(file.read())
			return sha1.hexdigest()

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

	def __make_dirs(self, path):
		try:
			os.makedirs(path)
		except OSError, e:
			if e.errno != 17:
				raise
			pass

	def __RunImpl(self, filepath):
		hash = self.__hash(filepath)
		if self.__lib:
			lib = self.__lib
			lib.PDFNet.Initialize()#self.__license)
			output_path = self.__output_dir

			if self.__centrailize_mode:
				output_path = os.path.join(self.__output_dir, hash, self.__ref_or_tar)

			if self.__centrailize_mode:
				if os.path.exists(output_path):
					self.__delete_all(output_path)
				else:
					self.__make_dirs(output_path)

			try:
				wordoc = lib.PDFDoc(filepath)
				draw = lib.PDFDraw()
				draw.SetDPI(92)

				it = wordoc.GetPageIterator()
				pagenum = 1
				prefix = os.path.commonprefix([filepath, self.__src_testdir])
				tail = os.path.relpath(os.path.splitext(filepath)[0], prefix)
				basename = os.path.basename(self.__src_testdir)
				while (it.HasNext()):
					output_file = os.path.join(output_path, basename, tail + "_" + str(pagenum) + ".png")
					draw.Export(it.Current(), output_file)
					print output_file
					it.Next()
					pagenum += 1
			except Exception as e:
				print e
		else:
			assert self.__bin_path
			fullbinpath = self.__bin_path

			output_dir = ''
			if self.__centrailize_mode:
				if self.__ref_or_tar == 'ref':
					output_dir = os.path.join(self.__output_dir, hash, self.__ref_or_tar, self.__ref_version_name)
				else:
					output_dir = os.path.join(self.__output_dir, hash, self.__ref_or_tar)
			else:
				prefix = os.path.commonprefix([filepath, self.__src_testdir])
				tail = os.path.relpath(os.path.dirname(filepath), prefix)
				basename = os.path.basename(self.__src_testdir)
				output_dir = os.path.join(self.__output_dir, basename, tail)
				output_dir = os.path.normpath(output_dir)

			if self.__centrailize_mode:
				# Only delete target runs
				if os.path.exists(output_dir) and self.__ref_or_tar != 'ref':
					self.__delete_all(output_dir)
				elif os.path.exists(output_dir) and self.__ref_or_tar == 'ref':
					return
				else:
					self.__make_dirs(output_dir)

			commands = ''
			if os.path.splitext(os.path.split(fullbinpath)[1])[0] == 'docpub':
				if os.path.splitext(filepath)[1] in ['.docx', '.pptx']:
					commands = [fullbinpath, '-f', 'pdf', '--builtin_docx=true', '--toimages=true', filepath, '-o', output_dir]
				else:
					commands = [fullbinpath, '-f', 'pdf', '--toimages=true', filepath, '-o', output_dir]
			else:
				commands = [fullbinpath, filepath, '-o', output_dir]

			process = subprocess.Popen(commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
			if process.returncode:
				print('An error occurred when converting: ' + filepath)

			stdout = process.communicate()[0]
			print(stdout)
			return

	def Run(self):
		pool = ThreadPool(self.__concurency)
		ret = pool.map(self.__RunImpl, self.__files)
		pool.close()
		pool.join()

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-f",
						"--files",
						type=str,
						default=None,
						help="The source test file path in a single string delimited by '|' directory")
	parser.add_argument("-v",
						"--version",
						action="store_true",
						help="Get the current version of the sdk")
	parser.add_argument("-s",
						"--src_dir",
						type=str,
						default=None,
						help="The source test directory")
	parser.add_argument("-o",
						"--out_dir",
						type=str,
						default=None,
						help="The output directory. Note: This option overrides --ref_out_dir, --tar_out_dir!")
	parser.add_argument("-ro",
						"--ref_out_dir",
						type=str,
						default=None,
						help="The reference output directory")
	parser.add_argument("-to",
						"--tar_out_dir",
						type=str,
						help="The target output direcotry")
	parser.add_argument("-c",
						"--concurency",
						type=int,
						default=4,
						help="The concurency value. Default is 4")
	parser.add_argument("-ursdk",
						"--use_ref_sdk",
						default=False,
						help="Specify whether to use reference PDFNet SDK")
	parser.add_argument("-utsdk",
						"--use_tar_sdk",
						default=False,
						help="Specify whether to use target PDFNet SDK")
	parser.add_argument("-rbinpath",
						"--ref_bin_path",
						type=str,
						help="Specify the binary executable path for reference")
	parser.add_argument("-tbinpath",
						"--tar_bin_path",
						type=str,
						help="Specify the binary executable path for reference")
	parser.add_argument("-rvname",
						"--ref_version_name",
						type=str,
						default='',
						help="Specify the reference output version_name")
	args = parser.parse_args()

	files = ''
	with open('allfiles.txt', 'r') as file:
		files = file.read()

	regression = Regression(ref_output_dir=args.ref_out_dir,
							tar_output_dir=args.tar_out_dir,
							files=files,
							version=args.version,
							src_dir=args.src_dir,
							concur=args.concurency,
							ref_use_sdk=args.use_ref_sdk,
							ref_bin_dir=args.ref_bin_path,
							tar_use_sdk=args.use_tar_sdk,
							tar_bin_dir=args.tar_bin_path,
							out_dir=args.out_dir,
							ref_version_name=args.ref_version_name)
	if not args.version:
		regression.Run()

if __name__ == '__main__':
	main()

