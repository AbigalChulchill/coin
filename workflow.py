import yaml
from setting import root_dir
import os
import time
import DataBaseManager
import importlib


def read_sql(sql_path):
    f = open(sql_path, 'r', encoding='utf8')
    return "".join(f.readlines())

def sql_task(source_parms, task_parms):
    source = source_parms['source']
    sql_path = os.path.join(root_dir, source, task_parms['sql_path'])
    print(sql_path)
    source_parms.update({'root_dir': root_dir})
    sql = read_sql(sql_path)%source_parms
    DataBaseManager.run_update(sql)

def python_task(source_parms, task_parms):
    source = source_parms['source']
    py_path = task_parms['py_path']
    mode_path = source+'.'+py_path.replace('.py', '').replace('/', '.')
    func_name = task_parms.get('func_parms', 'scrape')
    func_parms = task_parms.get('func_parms', {})
    mod = importlib.import_module(mode_path)
    mod = importlib.reload(mod)
    func = getattr(mod, func_name)
    return func(func_parms,source_parms, task_parms)


TASK_TYPE_MAPPING = {
    "sql": sql_task,
    "python": python_task
}



def read_workflow_def(source):
    path = os.path.join(root_dir, source, 'workflow.yaml')
    return yaml.safe_load(open(path))

# def task_run(source_parms:dict, task_parms:dict):
    # task_def = _read_workflow_def(source)



def launch_task(source, job):
    task_def = read_workflow_def(source)
    task_lst = task_def.get(job)
    source_parms = {
        'source': source,
        'job': job,
        'ts': int(time.time())
    }
    for task in task_lst:
        print(task)
        type = task.get('type')
        TASK_TYPE_MAPPING[type](source_parms, task)


if __name__ == "__main__":
    print(0)
    # python_task_test('coinmarketcap', 'scrape/test.py')
    # launch_task('coinmarketcap', 'coinmarketcap_list')
    launch_task('coinmarketcap', 'coinmarketcap_detail')


