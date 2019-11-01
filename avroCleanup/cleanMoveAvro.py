import os 

AVROFILE_DIR = "/home/julie/avroFiles/"

def main(): 
	for filename in os.listdir(AVROFILE_DIR): 
		dst = filename.replace(":", "_")
		src = AVROFILE_DIR + filename 
		dst = AVROFILE_DIR + dst 
		os.rename(src, dst) 

if __name__ == '__main__': 
	main() 
