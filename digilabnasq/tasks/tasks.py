from celery.task import task
from os import environ, pathsep
from subprocess import check_output, CalledProcessError
from shutil import rmtree
from tempfile import mkdtemp
import logging

from celeryconfig import DIGILABNAS_SCRIPT_PATH, WORKSPACE_PATH, PATH

logging.basicConfig(level=logging.INFO)

environ["PATH"] = PATH + pathsep + environ["PATH"]

READY_PATH = "{0}/5_Ready_for_script"
PREP_PATH = "{0}/6_Prep_to_bag"


def _digilabnas_wrapper(move_projects=False):
    """
    Internal wrapper to call the digilabnas script
    
    """
    logging.debug("Using script: {0}".format(DIGILABNAS_SCRIPT_PATH))
    logging.debug("Environment: {0}".format(environ))
    
    if not DIGILABNAS_SCRIPT_PATH:
        logging.error("Missing DIGILABNAS_SCRIPT_PATH")
        logging.error(environ)
        raise Exception("Digilabnas script path missing. Contact your administrator")

    if not WORKSPACE_PATH:
        logging.error("Missing WORKSPACE_PATH")
        logging.error(environ)
        raise Exception("Workspace not configured. Contact your administrator")

    source = READY_PATH.format(WORKSPACE_PATH)
    logging.info("Source path: {0}".format(source))
    
    tmpdir = mkdtemp(prefix="digilabnas_")
    logging.info("Temporay path: {0}".format(tmpdir))
    
    destination = PREP_PATH.format(WORKSPACE_PATH)
    logging.info("Destination path: {0}".format(destination))
    
    destroy = " --destroy" if move_projects else ""

    try:
        cmd_response = None
        cmd_response = check_output(
            "{0} --src {1} --dest {2}{3}".format(DIGILABNAS_SCRIPT_PATH, source, tmpdir, destroy),
            shell=True
        )
        logging.debug(cmd_response)
        if move_projects:
            mv_response = check_output(
                ["mv", "-v", "--no-clobber", "{0}/*".format(tmpdir), destination]
            )
    except CalledProcessError as err:
        logging.error(cmd_response)
        logging.error(err)
        logging.error(environ)
    finally:
        rmtree(tmpdir)

    if move_projects:
        return [cmd_response, mv_response]
    return cmd_response


@task
def apply_changes():
    """
    Normalizes project filenames and paths in preparation for bagging.

    This task calls the digilabnas script with --destroy option set.

    """
    return _digilabnas_wrapper(move_projects=True)


@task
def preview_changes():
    """
    Preview normalization of project filenames and paths in preparation for bagging.
    
    This task calls the digilabnas script in non-move_projects mode.
    
    """
    return _digilabnas_wrapper()
