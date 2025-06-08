'''
Title: configurationmanager.py
Author: George Clayton Bennett
Created : 05 June 2025

Purpose: Modularized file-based configuration via a Singlteon class. In this case, configuration is just credentials.

Attributes:
    - Load default config values from a TOML file
    - No fallbacks for secret.yaml files. 
    - Tracking history of changes to allow undo functionality if values are changed from the default during p(unexpected in this case).
'''


import toml
# import os
# from colletion import defaultdict

class ConfigurationManager:
    
    def __init__(self):
        self._instance = None