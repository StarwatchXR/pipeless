import os
import typer
from rich import print as rprint
import yaml
import shutil

app = typer.Typer()

@app.command(name="project", help="Create a new project")
def create_project(name: str):
    rprint(f"[yellow bold]Creating new project: {name}[/yellow bold]")

    # TODO: support more templates
    base_dir = os.path.dirname(__file__)
    project_template = base_dir + '/templates/default'
    try:
        shutil.copytree(project_template, name) # Also creates the directory
    except Exception as e:
        rprint(f"[red bold]An error occurred setting up the project template:[/red bold] {str(e)}")

    # TODO: all config should be possible to override form CLI options
    default_config = {
        'input': {
            'video': {
                'enable': True,
                'uri': 'some_hardcoded-uri'
            },
            'address': { # address where the input component runs for the nng connections
                'host': 'localhost',
                'port': 1234
            },
        },
        "output": {
            'video': {
                'enable': True,
                'uri': 'file:///tmp/my-video.mp4'
            },
            'address': { # address where the input component runs for the nng connections
                'host': 'localhost',
                'port': 1236
            },
        }
    }

    try:
        # TODO: override default config with user provided args
        new_config_file=open(f"{name}/config.yaml","w")
        yaml.dump(default_config, new_config_file)
        new_config_file.close()
        rprint("[yellow]Config file created.[/yellow]")
    except Exception as e:
        rprint(f"[red bold]An error occurred setting up the project config file:[/red bold] {str(e)}")
