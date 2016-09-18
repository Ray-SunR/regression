
class Task:
	def __init__(self, name, bin, src_file, out_file):
		self.__task_name = name
		self.__bin = bin
		self.__src_file = src_file
		self.__out_file = out_file