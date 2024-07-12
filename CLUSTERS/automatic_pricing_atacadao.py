import sys, os
import papermill as pm
import subprocess as sp
sys.path.append(os.path.expanduser('~'))
from alert_scripts.slack_notification_LOCAL import send_slack_notification
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(sys.path[0]))))
 
#print(sys.path)

parent_dir = os.getcwd()
parent_dir = os.path.dirname(parent_dir)
parent_dir = os.path.dirname(parent_dir)
os.chdir(parent_dir)

for city in ['SPO']:
    
    notebook_name = 'machine_pricing.ipynb'
    notebook_path = os.path.join(parent_dir, 'Growth', 'CLUSTERS', notebook_name)
    outcome_path = os.path.join(parent_dir, 'Growth','logs', f'CLUSTERS_aux.ipynb') # cuando lo haga por ciudad la puedo agregar aca
    
    try:
        pm.execute_notebook(notebook_path, outcome_path, log_output = True,parameters=dict(city=city))
        message = f'Exito en el notebook'
        people = {'ALL': ["felipe"]}.get('ALL', [])

        send_slack_notification('ALERT_PRICING_VARIATION', message, people, ':alert-green:')
    except:
        message = f'Falló el notebook {notebook_name}'
        people = {'ALL': ["felipe"]}.get('ALL', [])

        send_slack_notification('ALERT_PRICING_VARIATION', message, people, ':alert:')

        raise ValueError('Failed uploading CLUSTERS file')
        

# # Ruta al archivo concatenar_archivos.py
# archivo_script = os.path.join(parent_dir, 'Growth', 'CLTV', "concat_tiers.py")
# archivo_script2 = os.path.join(parent_dir, 'Growth', 'CLTV', "run_CLTV.py")

# # # Llama al script como si fuera ejecutado desde la terminal
# sp.run(["python3", archivo_script])
# sp.run(["python3", archivo_script2])