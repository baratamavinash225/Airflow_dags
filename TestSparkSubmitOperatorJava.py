import unittest
from airflow.models import DagBag

class TestSparkSubmitOperatorJava(unittest.TestCase):
    """Check HelloWorldDAG expectation"""

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of hello_world dag"""
        dag_id = 'SPARK_SUBMIT_TEST'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.task_count), 1)

    def test_contain_tasks(self):
        """Check task contains in hello_world dag"""
        dag_id = 'SPARK_SUBMIT_TEST'
        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['spark_submit_job'])


suite = unittest.TestLoader().loadTestsFromTestCase(TestSparkSubmitOperatorJava)
unittest.TextTestRunner(verbosity=2).run(suite)
