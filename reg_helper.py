from __future__ import print_function
__author__ = 'Renchen'

from multiprocessing.dummy import Pool as ThreadPool
import os.path
import argparse
import subprocess
import re
import json
import sys

class regression_core_task(object):
	def __init__(self,
				 files,
				 src_dir,
				 error_handler,
				 ref_output_dir=None,
				 tar_output_dir=None,
				 out_dir=None,
				 concur=4,
				 ref_bin_dir=None,
				 tar_bin_dir=None,
				 ref_version_name=None):

		self.__error_handler = error_handler
		self.__bin_path = None

		assert (ref_bin_dir or tar_bin_dir)

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

		self.__files = files
		self.__src_testdir = src_dir

		self.__ref_or_tar = ''
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
		assert self.__bin_path
		assert self.__files and self.__output_dir

		if self.__bin_path:
			assert os.path.exists(self.__bin_path)

		self.__concurency = concur


	def __hash(self, fpath):
		import hashlib
		try:
			with open(fpath, 'rb') as file:
				sha1 = hashlib.sha1(file.read())
				return sha1.hexdigest() + '_' + os.path.basename(fpath)
		except Exception as e:
			print(e)

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
		except Exception as e:
			pass

	def __run_impl(self, filepath):
		hash = self.__hash(filepath)
		if not hash:
			return
		assert self.__bin_path
		fullbinpath = self.__bin_path

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

		fullbinpath = os.path.abspath(fullbinpath)
		filepath = os.path.abspath(filepath)
		output_dir = os.path.abspath(output_dir)
		program_name = os.path.splitext(os.path.split(fullbinpath)[1])[0]
		sys.stdout.write('Converting: ' + filepath)
		sys.stdout.flush()
		if program_name == 'docpub':
			if os.path.splitext(filepath)[1].lower() in ['.docx', '.pptx']:
				commands = [fullbinpath, '-f', 'pdf', '--builtin_docx=true', '--toimages=true', filepath, '-o', output_dir]
			else:
				commands = [fullbinpath, '-f', 'pdf', '--toimages=true', filepath, '-o', output_dir]
		elif program_name == 'office2pdf':
			if os.path.splitext(filepath)[1].lower() in ['.docx', '.pptx', '.doc']:
				commands = [fullbinpath, '--qa', filepath, '-o', output_dir]
		else:
			commands = [fullbinpath, filepath, '-o', output_dir]
		process = subprocess.Popen(commands, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
		if process.returncode:
			self.__error_handler.write(filepath, object=self.__ref_or_tar, iscrash=True)
			print(self.__ref_or_tar + ': An error occurred when converting: ' + filepath)

		stdout = process.communicate()[0]
		try:
			if program_name == 'office2pdf':
				pattern = b'{bson_begin}\s(.*)\s{bson_end}'
				ret = re.search(pattern, stdout, re.DOTALL)
				if not ret:
					self.__error_handler.write(filepath, object=self.__ref_or_tar, iscrash=True)
				else:
					json_content = ret.group(1)
					bsonobj = json.loads(json_content.decode('utf-8'))
					if bsonobj['status'] == 'exception':
						self.__error_handler.write(filepath, bsonobj['exception_info']['failure_reason'], object=self.__ref_or_tar, isexception=True)

			sys.stdout.buffer.write(stdout)
			sys.stdout.flush()
		except Exception as e:
			print(e)
		return

	def Run(self):
		pool = ThreadPool(self.__concurency)
		ret = pool.map(self.__run_impl, self.__files)
		pool.close()
		pool.join()
