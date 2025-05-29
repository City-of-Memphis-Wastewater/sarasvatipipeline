#@echo off
#"---- $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ----" >> daemon_log.txt
poetry run python -c "from datetime import datetime; print('----', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '----')" >> daemon_log.txt
#where poetry
poetry --version
poetry run python -m projects.eds_to_rjn.scripts.daemon_runner >> daemon_log.txt 2>&1
#poetry run python -m projects.eds_to_rjn.scripts.main