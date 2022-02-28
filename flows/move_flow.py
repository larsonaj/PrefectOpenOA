import prefect
from prefect import Flow, task
import sys

sys.path.append('..')

import actions



@task(name="move_files")
def move_file():
    actions.movefile.move_text_files()

with Flow("move-doc") as flow:
    move_file()

flow.register(project_name='OpenOA')