import os
import subprocess
import sys

from lobpipe.spark import session


def test_workers_use_this_interpreter(spark):
    def where(_):
        import sys as worker_sys

        yield worker_sys.prefix

    assert spark.sparkContext.parallelize([0], 1).mapPartitions(where).collect() == [sys.prefix]


def test_session_keeps_an_interpreter_set_by_the_user(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", "/opt/other/bin/python")
    session(name="lobpipe-tests", master="local[2]", driver_memory="2g")
    assert os.environ["PYSPARK_PYTHON"] == "/opt/other/bin/python"


def test_spark_submit_settings_are_kept(tmp_path):
    import pyspark

    script = tmp_path / "job.py"
    script.write_text("import os\n"
                      "from lobpipe.spark import session\n"
                      "s = session(master='local[3]', driver_memory='3g')\n"
                      "print('MASTER', s.sparkContext.master)\n"
                      "print('MEMORY', s.sparkContext.getConf().get('spark.driver.memory', 'unset'))\n"
                      "print('PYTHON', os.environ.get('PYSPARK_PYTHON', 'unset'))\n")
    env = {k: v for k, v in os.environ.items() if k != "PYSPARK_PYTHON"}
    env.update(SPARK_HOME=os.path.dirname(pyspark.__file__), PYSPARK_DRIVER_PYTHON=sys.executable,
               PYTHONPATH=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    submit = os.path.join(os.path.dirname(sys.executable), "spark-submit")
    out = subprocess.run([submit, "--master", "local[1]", str(script)], env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr[-2000:]
    assert "MASTER local[1]" in out.stdout
    assert "MEMORY unset" in out.stdout
    assert "PYTHON unset" in out.stdout
