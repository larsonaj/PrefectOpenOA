import prefect
from prefect import Flow, task

from prefect.run_configs import UniversalRun
from prefect.storage import GitHub


storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"/flows/simple_github_flow.py",
    ref="dev",
    access_token_secret="GITHUB_API_KEY"
)

run_config = UniversalRun(labels=['DESKTOP-ETPQA0T'])


@task
def say_hello():
    logger = prefect.context.get("logger")
    logger.info("Hello, Cloud!")


with Flow("hello-flow", run_config=run_config, storage=storage) as flow:
    say_hello()