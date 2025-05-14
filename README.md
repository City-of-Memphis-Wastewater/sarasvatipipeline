# pipeline
The primary purpose of this project is to ease API access to Enterprise Data Server (EDS) machines set up by Emerson to complement an Emerson Ovation local system. 
Use-cases include data exchange with third-party contractors and also data access for in-house employees on work and personal devices.

*Scroll down to ***Rollout, setup, etc.*** to see information about dependencies, Poetry, and pyenv.*

## How to run pipeline

Check that the secrets.yaml file in the default-project is loading:
```
poetry run python -m src.pipeline.env
```
Recieve an export file of all the points known to your EDS:
```
poetry run python -m src.pipeline.api.eds demo-points-export
```
Run the existing eds_to_rjn project:
```
poetry run python -m projects.eds_to_rjn.scripts.main
```
Check connectivity:
```
poetry run python -m src.pipeline.api.eds ping
poetry run python -m src.pipeline.api.rjn ping
```
Other commands:
```
.\main.bat
.\main.ps1
```
## Known Errors
EDS Tabular Trend, blank list returned:
```
poetry run python -m src.pipeline.api.eds demo-trend
```
## secrets.yaml
Access will not work without a secrets.yaml file in /pipeline/projects/your-project-name/config/secrets.yaml

*API keys are specific to each the project, and can be referenced in the project scripts.*
You would edit the secrets.yaml file to specify your own EDS server and credentials.
Important: You need to VPN onto the same network as your server, EDS or otherwise, if it is not available to the outside world.

### Example secrets.yaml: 
```
eds_apis:
  MyServer1:
    url: "http://127.0.0.1:43084/api/v1/"
    username: "admin"
    password: ""
  MyServer2:
    url: "http://some-ip-address:port/api/v1/"
    username: "admin"
    password: ""

contractor_apis:
  MySpecialContractor:
    url: "https://contractor-api.com/v1/special/"
    client_id: "special-user"
    password: "2685steam"
```
The ***projects*** folder is designed to accomodate any custom projects that you might add. You can alter the default projet file by altering the directory name indicated in the /pipeline/projects/default-project.toml file.

# Future goals:
- Submit a pipeline library to PyPi.
- Generate a cookiecutter template for building new project directories.
- Use mulchcli (another ongoing repo) to guide users through setting up secrets.yaml files step-by-step.

# Rollout, setup, etc.
It is recommended that you use **pyenv** for setting the Python version and generating a virtual environment, though this is optional. 

To benefit from the pyproject.toml rollout for this project, **Poetry** is entirely necessary for installing requirements.

### Why pyenv?
venv and virtualenv are confusing, and pyenv is not confusing. With pyenv, it is easy to run many projects at once, with different requirements for each. 
You only need to install pyenv once on you system, and then you use it to access different versions of Python.

#### Note to anyone who doesn't yet understand the glory of virtual environments in Python: 
Do it. You do not want to install special requirements to your system version of Python.

### Why Poetry?
**Poetry** is my favorite dependency management tool. It is very easy. You only need to install it once. **Poetry** also has the benefit of generating a directory-specific virtual environment.

## Use pyenv to set your Python version 
(3.11.9 or other 3.11.xx).
### Install pyenv (skip if pyenv is already installed on your system)
How to install pyenv-win (https://github.com/pyenv-win/pyenv-win)
```
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```
It is worth it to make the pyenv command persistent in PowerShell, by editing the $profile file:
```
notepad $profile
```
to include something like:
```
$env:PYENV = "$HOME\.pyenv\pyenv-win"
$env:PYENV_ROOT = "$HOME\.pyenv\pyenv-win"
$env:PYENV_HOME = "$HOME\.pyenv\pyenv-win"
$env:Path += ";$env:PYENV\bin;$env:PYENV\shims"
	
# Initialize pyenv
#$pyenvInit = & $env:PYENV_HOME\bin\pyenv init --path
#Invoke-Expression $pyenvInit

# Manually set up the pyenv environment
function Invoke-PyenvInit {
    # Update PATH to include pyenv directories
    $pyenvShims = "$env:PYENV\shims"
    $pyenvBin = "$env:PYENV\bin"
    $env:PATH = "$pyenvBin;$pyenvShims;$env:PATH"
}

# Initialize pyenv
Invoke-PyenvInit
```
How to install pyenv on linux (https://github.com/pyenv/pyenv)
```
curl -fsSL https://pyenv.run | bash
```
To make the pyenv command persistent in the Bourne Again SHell, edit ~/.bashrc 
```
nano ~/.bashrc
```
to include something like:
```
export Path="$HOME/.local/bin:$PATH"
export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"
```
### Post-install, leverage the benefits of pyenv
Install Python 3.11.9 using pyenv:
```
# pyenv install --list # See all Python versions able with pyenv.
pyenv install 3.11.9
# pyenv local 3.11.9 # to set your current directory version
# pyenv global 3.11.9 # to set the assumed version in any directory
# pyenv global system # revert to your system installation as the assumed version
```
## Use Poetry to run deploy the requirements for this project 
How to install poetry (https://github.com/python-poetry/poetry)
```
Remove-Item Alias:curl # Solution to a common PowerShell issue 
curl -sSL https://install.python-poetry.org | python3
# Alternatively: 
// pip install poetry
# Or, even:
// pyenv exec pip install poetry
```
## Git clone pipeline, open source
```
git clone https://github.com/City-of-Memphis-Wastewater/pipeline.git
cd pipline
pyenv local 3.11.9 # to set your current directory version
```
Explicitly set poetry to use the local pyenv version.
```
poetry python list
# You'll see something like this:
>> 3.13.2  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.13.2/python3.13.exe
>> 3.13.2  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.13.2/python313.exe
>> 3.13.2  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.13.2/python3.exe
>> 3.13.2  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.13.2/python.exe
>> 3.11.9  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.11.9/python3.11.exe
>> 3.11.9  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.11.9/python311.exe
>> 3.11.9  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.11.9/python3.exe
>> 3.11.9  CPython        System  C:/Users/<user>/.pyenv/pyenv-win/versions/3.11.9/python.exe
# Copy and paste ~any~ of the comparable paths to pyenv 3.11.9 ...
poetry use C:\Users\<user>\.pyenv\pyenv-win\versions\3.11.9\python.exe
```
Pull the requirements from the pyproject.toml file for packagage installation.
```
poetry install 
# This is where the magic happens.
# When in doubt, run this again. 
# Especially if you get a ModuleNotFound warning.
# Sometimes it doesn't take until you use "poetry run python".
poetry run python
poetry run python -m src.pipeline.env
poetry run python -m src.pipeline.api.eds ping
poetry run python -m projects.eds_to_rjn.scripts.main
```
