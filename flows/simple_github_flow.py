import prefect
from prefect import Flow, task

from prefect.run_configs import UniversalRun
from prefect.storage import GitHub


storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"./Demo",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)



@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")

with Flow("hello-flow", run_config=UniversalRun(labels=['DESKTOP-ETPQA0T']), storage=storage) as flow:
    say_hello()

# Register the flow under the "tutorial" project
#flow.register(project_name="OpenOA")

#flow.storage = GitHub(repo="", path="")
#flow.run_config = DockerRun(image="test:latest", env={"KEY1":"VALUE1"})


## Tell docker container to look somewhere for api key