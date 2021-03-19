from argparse import ArgumentParser
from logging import getLogger


logger = getLogger(__name__)


def baq_main():
    p = ArgumentParser()
    args = p.parse_args()
    raise Exception('NIY')