#!/usr/bin/env python

from os.path import dirname, join

from pyspark import SparkContext


HERE = dirname(__file__)
INPUT_F = join(HERE, 'input.txt')
OUTPUT_DIR = join(HERE, 'output')


def main(app_name, input_fpath, output_dir):
    sc = SparkContext("local", app_name)
    input_txt = sc.textFile(input_fpath)
    input_txt.filter(
        lambda line: 'o' in line
    ).saveAsTextFile(
        output_dir
    )


if __name__ == '__main__':
    main("Hello World", INPUT_F, OUTPUT_DIR)
