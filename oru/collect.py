import argparse
import glob

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('input', nargs='+')
    p.add_argument('output')

