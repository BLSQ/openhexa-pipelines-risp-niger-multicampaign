"""
Domain config for orchestrate_pipelines_flow: which pipelines to run, and in what order.

Paths/OpenHEXA connection details for this pipeline live in openhexa_client.py
(OPENHEXA_BASE_URL) instead of here, since this pipeline has no workspace-file I/O of
its own - it only calls other pipelines via the OpenHEXA API.
"""

# Sequence
PIPELINE_ACTIONS = {
    "multi-campagne-compilation-des-cibles-et-de-la-structure-attendue": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/ZGIwOWUyNzItNzc3OC00MzhlLWFmNGEtZjgyMGE4MTdkNTY5OjF3eDUyMTpOeFBiWHJRRUE5LWFLd2dWZ2ZfTUY4Y0NhcmR3OUxkbmg3R3hLYnNwSTFj/run",
        "params": {},
    },
    "multi-campagne-extraction-des-unites-organisationnelles-iaso": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/ZWMyNzkxYWQtNGFjNy00ZTc4LTk0NzctOWEyNDljY2Q0ODAyOjF3dVBqQTpGQ25hWktEY3dZeEpZVE85aXJMcEE5ZzNCdVlrRTJOc3ZjbVZNNTNSYi1Z/run",
        "params": {},
    },
    "multi-campagne-extraction-des-donnees-du-formulaire-iaso": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/MTJhMzU0MzItMGIyZS00NTRmLTgzYzItZTljOGZkZmI3M2M1OjF3NTRESDpHYlJ3Qm04VG1rSklsYjNrVkZKZVhOZ05QbUJWZm5ZR2w2MG5rUE16M3Zr/run",
        "params": {},
    },
    "multi-campagne-traitement-des-donnees-du-formulaire-iaso": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/MTExOGRjZWEtMjZhOS00OTI1LTk4NWYtODM5YWYzNjk1YjZkOjF3NTRFVjprNm5tdFJFcHVMaWNQRTVVLTB4MlQyQXpJUnJ0MFNkWlpsTlZXRzRWc3Nv/run",
        "params": {},
    },
    "multi-campagne-construction-des-tableaux-pour-la-visualisation": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/NGE0NGY1MTctMTQxNS00MjA2LWExYTItN2VjYTJmY2M4ZDRmOjF3MTNYZzpDd0ppVDJFWmVvZVJXOGNjalhtLUFZRGVMdURpOTN3OUwwRWlLTmczT3JV/run",
        "params": {},
    },
    "multi-campagne-envoi-des-tables-de-visualisation-vers-la-base-de-donnees": {
        "type": "pipeline",
        "url": "https://api.openhexa.org/pipelines/MjQyYzU2MDEtYWIyNS00ZTgxLWI1NDEtYTU5ZGRlNWU0YWFjOjF3dEtjUzpvcUxNN0tmcHllM0RyalRTUFBUZVVobFNYUmxrMXhjSTR1UzVkUjQ2MTdr/run",
        "params": {},
    },
}
