"""I/O for Neo4j."""

from __future__ import annotations

from pathlib import Path

import click
from jinja2 import Environment, FileSystemLoader, select_autoescape

__all__ = ["write_neo4j"]

HERE = Path(__file__).parent.resolve()

TEMPLATES = HERE.joinpath("templates")
JINJA_ENV = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=select_autoescape())
STARTUP_TEMPLATE = JINJA_ENV.get_template("startup.sh")
DOCKERFILE_TEMPLATE = JINJA_ENV.get_template("Dockerfile")
RUN_ON_STARTUP_TEMPLATE = JINJA_ENV.get_template("run_on_startup.sh")

PYTHON_COMMAND = "python3.14"
STARTUP_SCRIPT_NAME = "startup.sh"
RUN_SCRIPT_NAME = "run_on_docker.sh"
DOCKERFILE_NAME = "Dockerfile"


def write_neo4j(
    directory: str | Path,
    *,
    node_paths: list[tuple[str, Path]],
    edge_paths: list[Path],
    docker_name: str,
    startup_script_name: str = STARTUP_SCRIPT_NAME,
    run_script_name: str = RUN_SCRIPT_NAME,
    dockerfile_name: str = "Dockerfile",
    pip_install: str | None = None,
) -> None:
    """Write all files needed to construct a Neo4j graph database from a set of mappings.

    :param directory: The directory to write nodes files, edge files, startup shell
        script (``startup.sh``), run script (``run_on_docker.sh``), and a Dockerfile
    :param docker_name: The name of the Docker image
    :param startup_script_name: The name of the startup script that the Dockerfile calls
    :param run_script_name: The name of the run script that you as the user should call
        to wrap building and running the Docker image
    :param dockerfile_name: The name of the Dockerfile produced
    :param pip_install: The package that's pip installed in the Docker file
    """
    directory = Path(directory).expanduser().resolve()
    directory.mkdir(exist_ok=True)

    startup_path = directory.joinpath(startup_script_name)
    startup_path.write_text(
        STARTUP_TEMPLATE.render(
            python=PYTHON_COMMAND,
        )
    )

    node_names = [(label, path.relative_to(directory)) for label, path in node_paths]
    edge_names = [path.relative_to(directory) for path in edge_paths]

    docker_path = directory.joinpath(dockerfile_name)
    docker_path.write_text(
        DOCKERFILE_TEMPLATE.render(
            node_names=node_names,
            edge_names=edge_names,
            pip_install=pip_install,
            python=PYTHON_COMMAND,
        )
    )

    run_path = directory.joinpath(run_script_name)
    run_path.write_text(
        RUN_ON_STARTUP_TEMPLATE.render(
            docker_name=docker_name,
            python=PYTHON_COMMAND,
        )
    )

    click.secho("Run Neo4j with the following:", fg="green")
    click.secho(f"  cd {run_path.parent.absolute()}")
    click.secho(f"  sh {run_script_name}")
