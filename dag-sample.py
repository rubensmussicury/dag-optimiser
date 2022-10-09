import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago

with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:


    t1 = MySqlOperator(
        task_id='complete-customer',
        sql='scripts/complete-customer.sql',
        dag=dag,
    )

    t2 = MySqlOperator(
        task_id='incomplete-customer',
        sql='scripts/incomplete-customer.sql',
        dag=dag,
    )

    t3 = MySqlOperator(
        task_id='removed-pii-customer',
        sql='scripts/removed-pii-customer.sql',
        dag=dag,
    )

    t4 = MySqlOperator(
        task_id='clicks-agg',
        sql='scripts/clicks-agg.sql',
        dag=dag,
    )

    t5 = MySqlOperator(
        task_id='order-agg',
        sql='scripts/order-agg.sql',
        dag=dag,
    )

    t6 = MySqlOperator(
        task_id='filter-clicks-high',
        sql='scripts/filter-clicks-high.sql',
        dag=dag,
    )

    t7 = MySqlOperator(
        task_id='filter-clicks-low',
        sql='scripts/filter-clicks-low.sql',
        dag=dag,
    )

    t8 = MySqlOperator(
        task_id='calculate-prc-customer',
        sql='scripts/calculate-prc-customer.sql',
        dag=dag,
    )

    t9 = MySqlOperator(
        task_id='calculate-click-value',
        sql='scripts/calculate-click-value.sql',
        dag=dag,
    )

    t10 = MySqlOperator(
        task_id='all-customer-status',
        sql='scripts/all-customer-status.sql',
        dag=dag,
    )

    t11 = MySqlOperator(
        task_id='audit-exec-customer',
        sql='scripts/audit-exec-customer.sql',
        dag=dag,
    )
	
    t12 = MySqlOperator(
        task_id='audit-exec-click',
        sql='scripts/audit-exec-click.sql',
        dag=dag,
    )
	
    t12 = MySqlOperator(
        task_id='audit-exec-order',
        sql='scripts/audit-exec-order.sql',
        dag=dag,
    )
	
