#!/usr/bin/env python

import argparse
import os, sys
from sqlalchemy import create_engine
from jsearch.common.tables import *
import jsearch.common.alembic_utils as alembic
from jsearch.common import testutils


class Manage(object):

    def __init__(self):
        self.__script_name = os.path.basename(__file__)
        parser = argparse.ArgumentParser(
            usage='''
usage: {} <command> [<args>]
Commands:
init\t\tInitialize a new scripts directory.
revision\tCreate a new revision file.
upgrade\t\tUpgrade to a later version.
downgrade\tRevert to a previous version.
merge\t\tMerge two revisions together. Creates a new migration file.'''
                .format(self.__script_name))

        parser.add_argument('command', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def init(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: {} init <directory>
positional arguments:
directory\t\tlocation of scripts directory'''
                .format(self.__script_name))

        parser.add_argument('directory', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print('Running alembic init {}'.format(args.directory))
        alembic.init(args.directory)

    def revision(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: {} revision [-db connection_string] [-h] [-m MESSAGE]
Arguments:
-h, --help\t\tshow this help message and exit
-db connection_string\tConnection URL
-m MESSAGE, --message MESSAGE'''
                .format(self.__script_name))

        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        parser.add_argument('-m', action='store', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print('Running alembic revision --autogenerate -m "{}". db: {}'.format(args.m, args.db))
        alembic.revision(args.db, args.m, True)

    def upgrade(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: {} upgrade [-h] revision
positional arguments:
  revision    revision identifier'''
                .format(self.__script_name))

        parser.add_argument('revision', help=argparse.SUPPRESS)
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running alembic upgrade {}. db: {}'.format(args.revision, args.db or os.getenv('JSEARCH_MAIN_DB')))
        alembic.upgrade(args.db or os.getenv('JSEARCH_MAIN_DB'), args.revision)

    def downgrade(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: alembic downgrade [-h] revision
positional arguments:
  revision    revision identifier'''
                .format(self.__script_name))

        parser.add_argument('revision', help=argparse.SUPPRESS)
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running alembic downgrade {}. db: {}'.format(args.revision, args.db))
        alembic.downgrade(args.db, args.revision)


    def json_dump(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: json_dump [-h]'''
                .format(self.__script_name))
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running json_dump. db: {}'.format(args.db))
        alembic.json_dump(args.db)

    def add_test_contract(self):
        parser = argparse.ArgumentParser(
            usage='''
usage: json_dump [-h]'''
                .format(self.__script_name))
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        parser.add_argument('-address', action='store', help=argparse.SUPPRESS)
        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running add_test_contract. db: {}'.format(args.db))
        testutils.add_test_contract(args.db, args.address)
if __name__ == '__main__':
    Manage()