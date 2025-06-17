import os
import logging
import importlib
import sys
from src.pipeline.projectmanager import ProjectManager

# Paths
RUNTIME_DIR = os.path.join(os.getenv("APPDATA", os.path.expanduser("~/.config")), "memphis_pipeline", "runtime")
RUNNING_FLAG = os.path.join(RUNTIME_DIR, "daemon_running.flag")
STATUS_LOG = os.path.join(RUNTIME_DIR, "daemon_status.log")

# Ensure runtime dir exists
os.makedirs(RUNTIME_DIR, exist_ok=True)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def log_status(message: str):
    """Logs a message both to a file and to the console."""
    with open(STATUS_LOG, "a") as f:
        f.write(f"{message}\n")
    logger.info(message)

def write_running_flag():
    """Creates a flag file indicating the daemon is running."""
    with open(RUNNING_FLAG, "w") as f:
        f.write("running")

def remove_running_flag():
    """Removes the running flag file if it exists."""
    if os.path.exists(RUNNING_FLAG):
        os.remove(RUNNING_FLAG)

def load_module_from_path(module_path: str, module_name: str = "main"):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        logger.error(f"Cannot load module from path: {module_path}")
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def start_daemon():
    """
    Starts the daemon process for the identified project and runs its `run_daemon()` function.
    """
    # Get project directory via ProjectManager
    project_name = ProjectManager.identify_default_project()
    project_manager = ProjectManager(project_name)
    project_dir = project_manager.get_project_dir()
    print(f"project_dir = {project_dir}")
    
    """
    # Dynamically import the main module using the project directory
    module_path = f"{project_dir}.main"
    project_module = importlib.import_module(module_path)
    # Get and call the "main()" function from the project module
    main = getattr(project_module, "main")
    write_running_flag()
    main()
    remove_running_flag()
    """
    # Dynamically import the main module using the project directory
    #module_path = f"{project_dir}.scripts.main"
    module_path = project_manager.get_scripts_file_path(filename="main.py")
    # Dynamically load the module from that path
    try:
        project_module = load_module_from_path(module_path)
        if project_module is None:
            return
    except ModuleNotFoundError as e:
        logger.error(f"Could not import module '{module_path}': {e}")
        return

    # Try to get the main() function, with a fallback if not present
    try:
        main_fn = getattr(project_module, "main")
    except AttributeError:
        logger.warning(f"Module '{module_path}' does not define a 'main()' function.")
        fallback_action(project_name)
        return

    # Run the main function with daemon state handling
    write_running_flag()
    try:
        main_fn()
    finally:
        remove_running_flag()
def fallback_action(project_name: str):

    logger.info(f"Running fallback action for project '{project_name}'.")

    # For now, just log. You could alternatively:
    # - Import and run a sketch_maxson()
    # - Raise a RuntimeError
    # - Or exit with sys.exit(1)
    print(f"[INFO] No 'main()' found for '{project_name}'. Nothing was run.")

def stop_daemon():
    """Stops the daemon by removing the running flag."""
    remove_running_flag()
    log_status("Daemon stopped.")

def status_daemon():
    """Returns the status of the daemon."""
    if os.path.exists(RUNNING_FLAG):
        log_status("Daemon is currently running.")
        print("RUNNING")
    else:
        log_status("Daemon is not running.")
        print("STOPPED")

def main_cli():
    """CLI interface to control the daemon."""
    if len(sys.argv) < 2:
        print("Usage: python -m src.pipeline.daemon.controller [-start | -stop | -status]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "-start":
        start_daemon()
    elif command == "-stop":
        stop_daemon()
    elif command == "-status":
        status_daemon()
    else:
        print("Usage: python -m src.pipeline.daemon.controller [-start | -stop | -status]")
        sys.exit(1)

if __name__ == "__main__":
    """
    poetry run python -m src.pipeline.daemon.controller -start  # To start the daemon
    poetry run python -m src.pipeline.daemon.controller -stop   # To stop the daemon
    poetry run python -m src.pipeline.daemon.controller -status # To check the daemon status
    """
    main_cli()
