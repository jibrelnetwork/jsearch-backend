#!/usr/bin/env python

import argparse
import os
import sys
from pathlib import Path

import jsearch.common.alembic_utils as alembic


class Manage(object):
    script_name = Path

    def __init__(self):
        self.script_name = os.path.basename(__file__)
        parser = argparse.ArgumentParser(
            usage=(
                '\n'
                f'usage: {self.script_name} <command> [<args>]\n'
                'Commands:\n'
                'init\t\tInitialize a new scripts directory.\n'
                'revision\tCreate a new revision file.\n'
                'upgrade\t\tUpgrade to a later version.\n'
                'downgrade\tRevert to a previous version.\n'
                'merge\t\tMerge two revisions together. Creates a new migration file.'
            )
        )
        parser.add_argument('command', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def init(self):
        parser = argparse.ArgumentParser(
            usage=(
                '\n'
                f'usage: {self.script_name} init <directory>\n'
                'positional arguments:\n'
                'directory\t\tlocation of scripts directory'
            )
        )
        parser.add_argument('directory', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print('Running alembic init {}'.format(args.directory))
        alembic.init(args.directory)

    def revision(self):
        parser = argparse.ArgumentParser(
            usage=(
                '\n'
                'usage: {} revision [-db connection_string] [-h] [-m MESSAGE]\n'
                'Arguments:\n'
                '-h, --help\t\tshow this help message and exit\n'
                '-db connection_string\tConnection URL\n'
                '-m MESSAGE, --message MESSAGE'
            )
        )

        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        parser.add_argument('-m', action='store', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print('Running alembic revision --autogenerate -m "{}". db: {}'.format(args.m, args.db))
        alembic.revision(args.db, args.m, True)

    def upgrade(self):
        parser = argparse.ArgumentParser(
            usage=(
                '\n'
                f'usage: {self.script_name} upgrade [-h] revision\n'
                'positional arguments:\n'
                '  revision    revision identifier'
            )
        )

        parser.add_argument('revision', help=argparse.SUPPRESS)
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running alembic upgrade {}. db: {}'.format(args.revision, args.db or os.getenv('JSEARCH_MAIN_DB')))
        alembic.upgrade(args.db or os.getenv('JSEARCH_MAIN_DB'), args.revision)

    def downgrade(self):
        parser = argparse.ArgumentParser(
            usage=(
                '\n'
                f'usage: {self.script_name} [-h] revision\n'
                'positional arguments:\n'
                '  revision    revision identifier'
            )
        )

        parser.add_argument('revision', help=argparse.SUPPRESS)
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running alembic downgrade {}. db: {}'.format(args.revision, args.db))
        alembic.downgrade(args.db, args.revision)

    def json_dump(self):
        parser = argparse.ArgumentParser(usage=f"usage {self.script_name} [-h]")
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        parser.add_argument('-out', action='store', default=None, help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running json_dump. db: {}'.format(args.db))
        alembic.json_dump(args.db, args.out)

    def add_test_contract(self):
        from jsearch.tests.utils import add_test_contract
        from jsearch.tests.plugins.tokens.fuck_token import FuckTokenSource

        parser = argparse.ArgumentParser(usage=f"usage: {self.script_name} [-h]")
        parser.add_argument('-db', action='store', help=argparse.SUPPRESS)
        parser.add_argument('-address', action='store', help=argparse.SUPPRESS)

        args = parser.parse_args(sys.argv[2:])
        print(args)
        print('Running add_test_contract. db: {}'.format(args.db))

        add_test_contract(args.db, args.address, FuckTokenSource.load())


if __name__ == '__main__':
    Manage()
