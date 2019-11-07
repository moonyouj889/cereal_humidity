import os 
import argparse


def main(avroDir):
    for filename in os.listdir(avroDir): 
        dst = filename.replace(":", "_")
        src = avroDir + filename 
        dst = avroDir + dst 
        os.rename(src, dst) 


if __name__ == '__main__': 
    parser = argparse.ArgumentParser(
        description='Bulk clean Avro filenames for HDFS compatibility')
    parser.add_argument(
        '--avroDir', required=True, type=int,
        help=('full directory of Avro files produced by Beam'))
    args = parser.parse_args()
	main(args.avroDir) 
    
