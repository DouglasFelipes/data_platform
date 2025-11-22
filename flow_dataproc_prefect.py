import google.auth
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.types import Batch, PySparkBatch
from prefect import flow, task
from prefect.runtime import flow_run  # <-- 1. IMPORTADO O RUNTIME


@task
def run_dataproc_serverless():
    credentials, project_id = google.auth.default()
    region = "us-central1"

    client = dataproc_v1.BatchControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    runtime_config = {
        "properties": {
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
            "spark.driver.memoryOverhead": "1g",
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.executor.memoryOverhead": "1g",
            "spark.executor.instances": "2",
        }
    }

    pyspark_file = "gs://prefect-dgzflows/jobs/job_pyspark.py"

    batch = Batch(
        pyspark_batch=PySparkBatch(main_python_file_uri=pyspark_file, args=[]),
        runtime_config=runtime_config,
    )

    parent = f"projects/{project_id}/locations/{region}"

    # --- INÍCIO DA MUDANÇA ---

    # 2. Pega o ID único desta execução do flow
    # O ID será algo como 'a1b2c3d4-e5f6-...'
    flow_id = flow_run.id

    # 3. Cria um batch_id único e rastreável
    batch_id_unico = f"prefect-dataproc-job-{flow_id}"

    # --- FIM DA MUDANÇA ---

    # CRIA A OPERAÇÃO
    operation = client.create_batch(
        request={
            "parent": parent,
            "batch_id": batch_id_unico,  # <-- 4. USA O ID ÚNICO AQUI
            "batch": batch,
        }
    )

    # ESPERA O JOB TERMINAR E PEGA O BATCH FINAL
    batch_response = operation.result()

    return batch_response.name


@flow(name="dataproc-flow")
def dataproc_flow():
    print("Iniciando job no Dataproc Serverless...")
    job_id = run_dataproc_serverless()
    print(f"Job enviado e finalizado: {job_id}")


if __name__ == "__main__":
    dataproc_flow()
