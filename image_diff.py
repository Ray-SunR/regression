from PIL import ImageChops
from PIL import Image
import os
import argparse
import json

def ImageCompare(im1, im2):
		try:
			diffcount = 0.0

			imgcompdiff = ImageChops.difference (im1, im2)
			diffboundrect = imgcompdiff.getbbox ()
			imgdiffcrop = imgcompdiff.crop (diffboundrect)

			data = imgdiffcrop.getdata()

			seq = []
			for row in data:
				seq += list(row)

			for i in range (0, imgdiffcrop.size[0] * imgdiffcrop.size[1] * 3, 3):
				if seq[i] != 0 or seq[i+1] != 0 or seq[i+2] != 0:
					diffcount = diffcount + 1.0

			diffImgLen = imgcompdiff.size[0] * imgcompdiff.size[1] * 1.0
			diffpercent = (diffcount * 100) / diffImgLen
			return diffpercent
		except IOError:
			#raise Exception, 'Input file does not exist'
			pass

def RunImageDiffImpl(tuple):
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
				#enhancer = ImageEnhance.Sharpness(img2)
				#img2 = enhancer.enhance(8)
				diff_img = ImageChops.difference(im1, img2)
				diff_image_path = os.path.join(outpath, os.path.basename(path1))


				diff_percentage = ImageCompare(im1, img2)
				if diff_percentage != 0 or True:
					# Only save diff if they are different
					diff_img.save(diff_image_path, 'PNG')
					msg = {}
					msg['diff_image_path'] = diff_image_path
					msg['diff_percentage'] = diff_percentage

					print(json.dumps(msg))


		except Exception as e:
			print(e)

def main():
	try:
		parser = argparse.ArgumentParser()
		parser.add_argument("-f1",
							"--file1",
							type=str,
							default=None,
							help="The reference path")
		parser.add_argument("-f2",
							"--file2",
							type=str,
							default=None,
							help="The target path")
		parser.add_argument("-o",
							"--output",
							type=str,
							default=None,
							help="The ouput path")
		args = parser.parse_args()
		tuple = (args.file1, args.file2, args.output)

		RunImageDiffImpl(tuple)
	except Exception as e:
		print(e)

if __name__ == '__main__':
	main()