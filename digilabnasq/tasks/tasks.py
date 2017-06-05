from celery.task import task
from os import environ, pathsep
from subprocess import check_call, check_output, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging

from celeryconfig import DIGILABNAS_SCRIPT_PATH, WORKSPACE_PATH, PATH

logging.basicConfig(level=logging.INFO)

environ["PATH"] = PATH + pathsep + environ["PATH"]


@task()
def _digilabnas_wrapper(source="", destination="", destructive=False):
    """
    Internal wrapper to call the digilabnas script
    
    """
    logging.debug("Using script: {0}".format(DIGILABNAS_SCRIPT_PATH))
    logging.debug("Environment: {0}".format(environ))
    
    if not DIGILABNAS_SCRIPT_PATH:
        logging.error("Missing DIGILABNAS_SCRIPT_PATH")
        logging.error(environ)
        raise Exception("Digilabnas script path missing. Contact your administrator")

    if source == "":
        source = WORKSPACE_PATH
    logging.info("Source path: {0}".format(source))
    
    if destination == "":
        destination = mkdtemp(prefix="digilabnas_")
    logging.info("Destination path: {0}".format(destination))
    
    destroy = " --destroy" if destructive else ""

    try:
        cmd_response = None
        cmd_response = check_output(
            "{0} --src {1} --dest {2}{3}".format(DIGILABNAS_SCRIPT_PATH, source, destination, destroy),
            shell=True
        )
        logging.debug(cmd_response)
    except CalledProcessError as err:
        logging.error(cmd_response)
        logging.error(err)
        logging.error(environ)
    return cmd_response


@task
def apply_changes():
    """
    Normalizes project filenames and paths in preparation for bagging.

    This task calls the digilabnas script with --destroy option set.

    """
    return _digilabnas_wrapper(destructive=True)


@task
def preview_changes():
    """
    Preview normalization of project filenames and paths in preparation for bagging.
    
    This task calls the digilabnas script in non-destructive mode.
    
    """
    return _digilabnas_wrapper()
