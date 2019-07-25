import unittest
from airflow.models import DagBag
from datetime import datetime

class TestHelloWorldDag(unittest.TestCase):
    """Check HelloWorldDAG expectation"""

    def setUp(self):
        self.dagbag = DagBag()
        self.dag_id = 'prokarma_hello_world'
        self.dag = self.dagbag.get_dag(self.dag_id)

    def test_task_count(self):
        """Check task count of hello_world dag"""
        self.assertEqual(self.dag.task_count, 2)

    def test_contain_tasks(self):
        """Check task contains in hello_world dag"""
        tasks = self.dag.tasks
        task_ids = list(map(lambda task: task.task_id, tasks))
        self.assertListEqual(task_ids, ['dummy_task', 'hello_task'])
    
    def test_dag_contains_startdate(self):
        start_date = self.dag.start_date
        self.assertEqual(start_date,datetime(2017, 3, 20))
    
    def test_dag_contains_schedule_interval(self):
        schedule_interval = self.dag.schedule_interval
        self.assertEqual(schedule_interval, '0 12 * * *')
    
    


suite = unittest.TestLoader().loadTestsFromTestCase(TestHelloWorldDag)
unittest.TextTestRunner(verbosity=2).run(suite)
