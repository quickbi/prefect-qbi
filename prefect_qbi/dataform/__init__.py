import logging
import sys
import time

from google.api_core import exceptions, retry
from google.cloud import dataform_v1beta1
from prefect import get_run_logger


def get_logger():
    try:
        return get_run_logger()
    except Exception:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        return logger


def _compile(
    client: dataform_v1beta1.DataformClient,
    project: str,
    location: str,
    repository: str,
) -> str:
    logger = get_logger()

    repository_path = (
        f"projects/{project}/locations/{location}/repositories/{repository}"
    )

    def should_retry(exc):
        # The API sometimes returns error 400 with message:
        # The remote repository [...] closed connection during remote operation.
        default_should_retry = retry.if_transient_error(exc)
        return default_should_retry or isinstance(exc, exceptions.InvalidArgument)

    compilation_result = client.create_compilation_result(
        parent=repository_path,
        compilation_result=dataform_v1beta1.CompilationResult(
            git_commitish="main",
            code_compilation_config=dataform_v1beta1.CodeCompilationConfig(
                default_schema="reporting",
            ),
        ),
        retry=retry.Retry(predicate=should_retry),
    )

    if compilation_result.compilation_errors:
        for compilation_error in compilation_result.compilation_errors:
            logger.error("Compilation error: %s", compilation_error.message)
        raise Exception("Compilation reported errors")

    compilation_result_path = compilation_result.name

    return compilation_result_path.removeprefix(
        f"{repository_path}/compilationResults/"
    )


def _execute(
    client: dataform_v1beta1.DataformClient,
    project: str,
    location: str,
    repository: str,
    compilation_result: str,
):
    logger = get_logger()

    repository_path = (
        f"projects/{project}/locations/{location}/repositories/{repository}"
    )
    compilation_result_path = (
        f"{repository_path}/compilationResults/{compilation_result}"
    )

    response = client.query_compilation_result_actions(
        request=dataform_v1beta1.QueryCompilationResultActionsRequest(
            name=compilation_result_path,
        ),
    )
    for compilation_result_action in response:
        break
    else:
        # Trying to create workflow invocation with no actions fails.
        logger.warning("No actions found! Skipping execution...")
        return

    workflow_invocation = client.create_workflow_invocation(
        parent=f"projects/{project}/locations/{location}/repositories/{repository}",
        workflow_invocation=dataform_v1beta1.WorkflowInvocation(
            compilation_result=compilation_result_path,
        ),
    )
    while (
        workflow_invocation.state == dataform_v1beta1.WorkflowInvocation.State.RUNNING
    ):
        time.sleep(5)
        workflow_invocation = client.get_workflow_invocation(
            name=workflow_invocation.name,
        )
    if workflow_invocation.state != dataform_v1beta1.WorkflowInvocation.State.SUCCEEDED:
        response = client.query_workflow_invocation_actions(
            request=dataform_v1beta1.QueryWorkflowInvocationActionsRequest(
                name=workflow_invocation.name,
            ),
        )
        for workflow_invocation_action in response:
            if (
                workflow_invocation_action.state
                == dataform_v1beta1.WorkflowInvocationAction.State.FAILED
            ):
                logger.error(
                    "Execution error in %s: %s",
                    workflow_invocation_action.canonical_target.name,
                    workflow_invocation_action.failure_reason,
                )
        raise Exception("Execution terminated unsuccefully")


def run(
    client: dataform_v1beta1.DataformClient,
    project: str,
    location: str,
    repository: str,
):
    compilation_result = _compile(client, project, location, repository)
    _execute(client, project, location, repository, compilation_result)
