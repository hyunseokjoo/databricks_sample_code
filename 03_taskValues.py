
# set sample
dbutils.jobs.taskValues.set(key="variable-name", value="value")
# get sample
dbutils.jobs.taskValues.get(taskKey="task-name-dbx-workflow",
                            key="variable-name",
                            default=7,
                            debugValue="debug-value")


# setting은 task UI에서 parameter add 및 설정 후
value = dbutils.widgets.get("key")
