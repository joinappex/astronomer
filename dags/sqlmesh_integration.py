from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow("bigquery", default_catalog="appex-data",
    # engine_operator_args={
    #     "bigquery_conn_id": "sqlmesh_google_cloud_bigquery_default",
    #     "location": "US"
    # },
)

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
