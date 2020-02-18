#!/usr/bin/env python
import subprocess
import sys
from typing import NamedTuple, Any

import click
from sqlalchemy import create_engine

from jsearch.utils import get_alembic_version, get_goose_version

MIGRATIONS_FOLDER = './migrations'


class GooseWrapper(NamedTuple):
    dsn: str
    dir: str

    def _call(self, *args: Any) -> None:
        cmd = f"goose -dir {self.dir} postgres {self.dsn}?sslmode=disable {' '.join(args)}"
        sys.stdout.write('CMD: {cmd}\n'.format(cmd=cmd.replace(self.dsn, '*' * len(self.dsn))))

        result = subprocess.run(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
        if result.returncode != 0:
            sys.exit(1)

    def create(self, message: str) -> None:
        self._call("create", f'"{message}"', "sql")

    def up(self) -> None:
        self._call("up")

    def up_by_one(self):
        self._call("up-by-one")

    def up_to(self, version: str):
        self._call("up-to", version)

    def down(self) -> None:
        self._call("down")

    def down_to(self, version: str):
        self._call("down-to", version)

    def status(self) -> None:
        self._call("status")

    def version(self) -> None:
        self._call("version")


def init_goose(db_dsn: str) -> None:
    query = """
    CREATE TABLE IF NOT EXISTS goose_db_version (
        id serial NOT NULL,
        version_id bigint NOT NULL,
        is_applied boolean NOT NULL,
        tstamp timestamp NULL default now(),
        PRIMARY KEY(id)
    );
    """
    insert_query = """
    INSERT INTO goose_db_version (version_id, is_applied) VALUES (%s, %s);
    """
    engine = create_engine(db_dsn)
    engine.execute(query)
    engine.execute(insert_query, "20191015163320", True)


@click.group()
@click.option('--dsn', envvar='JSEARCH_MAIN_DB')
@click.option('--dir', default=MIGRATIONS_FOLDER)
@click.pass_context
def cli(ctx, dsn, dir):
    ctx.obj = GooseWrapper(dsn=dsn, dir=dir)


@click.command()
@click.option("-m", "--message", default="", help="migration title")
@click.pass_context
def create(ctx, message):
    ctx.obj.create(message)


@click.command()
@click.pass_context
def status(ctx):
    ctx.obj.status()


@click.command()
@click.pass_context
def version(ctx):
    ctx.obj.version()


@click.command()
@click.pass_context
def up(ctx):
    ctx.obj.up()


@click.command()
@click.pass_context
def up_by_one(ctx):
    ctx.obj.up_by_one()


@click.command()
@click.pass_context
@click.option("-m", "--message", default="", help="migration title")
def up_to(ctx, version: str):
    ctx.obj.up_to(version)


@click.command()
@click.pass_context
def down(ctx):
    ctx.obj.down()


@click.command()
@click.pass_context
@click.option("-v", "--version", default="", help="version")
def down_to(ctx, version: str):
    ctx.obj.down_to(version)


@click.command()
@click.pass_context
def init(ctx):
    sys.stdout.write(f'Try to find migrations schema...\n')

    alembic_version = get_alembic_version(ctx.obj.dsn)
    goose_version = get_goose_version(ctx.obj.dsn)

    if alembic_version and not goose_version:
        sys.stdout.write(f'Alembic detected, switch to goose...\n')

        init_goose(ctx.obj.dsn)
        goose_version = get_goose_version(ctx.obj.dsn)
        sys.stdout.write(f'Migrations is completed.\n')
        sys.stdout.write(f'[GOOSE]: {goose_version}\n')
    elif not goose_version:
        sys.stdout.write(f'Alembic was not detected. Schema is empty, need to apply goose migrations...\n')
        ctx.obj.up()

        goose_version = get_goose_version(ctx.obj.dsn)
        sys.stdout.write(f'Migrations is completed.\n')
        sys.stdout.write(f'[GOOSE]: {goose_version}\n')
    else:
        sys.stdout.write(f'[GOOSE]: {goose_version}\n')
        sys.stdout.write(f'[ALEMBIC]: {alembic_version}\n')


cli.add_command(up)
cli.add_command(up_by_one)
cli.add_command(up_to)
cli.add_command(down)
cli.add_command(down_to)
cli.add_command(status)
cli.add_command(version)
cli.add_command(create)
cli.add_command(init)

if __name__ == '__main__':
    cli()
