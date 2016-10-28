__author__ = 'Renchen'
import operator

class errorhandler:
	def __init__(self, name='error.txt'):
		self.__name = name
		self.__filehandle = open(self.__name, 'wb')
		self.__crashes = []

		# This is a map between exception message and file name
		self.__exceptions = {}
		self.__missing = []

	def writemessage(self, msg):
		self.__filehandle.write(msg)
		self.__filehandle.write('\n'.encode('utf-8'))

	def write(self, filepath, message='', object='', iscrash=False, ismissing=False, isexception=False):
		'''
		object: 'ref' or 'tar' or ''
		'''

		if iscrash:
			self.__crashes.append(filepath)
			self.__filehandle.write(('(%s) Crash when converting: %s\n' % (object, filepath)).encode('utf-8'))
		elif isexception:
			assert(message)
			if message not in self.__exceptions:
				self.__exceptions[message] = [filepath]
			else:
				self.__exceptions[message].append(filepath)
			self.__filehandle.write(('(%s) Exception: %s occurs when converting: %s\n' % (object, message, filepath)).encode('utf-8'))
		else:
			self.__missing.append(filepath)
			self.__filehandle.write(('(%s) %s is missing' % (object, filepath)).encode('utf-8'))

	def crashes(self):
		return self.__crashes

	def __exception_sort(self, item):
		return len(item[1])

	def exceptions(self):
		return sorted(self.__exceptions.items(), key=self.__exception_sort)

	def missing(self):
		return self.__missing

	def numofexceptions(self):
		ret = 0
		for item in self.__exceptions.items():
			ret = ret + len(item[1])
		return ret