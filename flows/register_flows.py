from prefect import Flow
from prefect.run_configs import UniversalRun
from prefect.storage import Docker

from Demo import demo_flow

storage = Git(
    repo_host="github.com",
    repo='larsonaj/PrefectOpenOA',
    flow_path=f"demo_flow.py",
    flow_name=demo_flow,
    branch_name=main,
    tag=build_tag,
    git_token_secret_name=git_token_secret_name,
    git_token_username=git_token_username
)

with Flow("git-storage-example") as flow:
    

# Add tasks to flow here...
# Run on Kubernetes with a custom resource configuration
flow.run_config = UniversalRun(labels=['AGENT_NAME'])

# Store the flow in a docker image
flow.storage = Docker()
