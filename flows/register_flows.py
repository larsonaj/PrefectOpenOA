from prefect import Flow
from prefect.run_configs import UniversalRun
from prefect.storage import GitHub

from Demo import demo_flow

storage = GitHub(
    repo='larsonaj/PrefectOpenOA',
    path=f"demo_flow.py",
    ref="main",
    access_token_secret=git_token_secret_name
)

run_config = UniversalRun(labels=['AGENT_NAME'])




with Flow("git-storage-example", storage=storage, run_config=run_config) as flow:
    demo_flow()