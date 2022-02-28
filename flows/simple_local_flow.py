import prefect
from prefect import Flow, task
from prefect.run_configs import UniversalRun

@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

with Flow("hello-flow", run_config=UniversalRun(labels=['DESKTOP-ETPQA0T'])) as flow:
    say_hello()

# Register the flow under the "tutorial" project
#flow.register(project_name="OpenOA")

#flow.storage = GitHub(repo="", path="")
#flow.run_config = DockerRun(image="test:latest", env={"KEY1":"VALUE1"})


## Tell docker container to look somewhere for api key