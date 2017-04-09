import sys

if __name__ == "__main__":
	#arg1 - file
	#arg2 - line of file
	#arg2 - first index (inclusive)
	#arg3 - second index (exclusive)

	fp = open(sys.argv[1])

	searchline = ""
	for i, line in enumerate(fp):
		if i == int(sys.argv[2]):
			searchline = line

	print(searchline[int(sys.argv[3])-1:int(sys.argv[4])-1])
	
