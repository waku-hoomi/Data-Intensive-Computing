from urban_platform.analytics import experiments


def test_benchmark_retains_trials_and_checks_equality(spark,tmp_path,monkeypatch):
    monkeypatch.setattr(experiments,"ROOT",tmp_path)
    monkeypatch.setattr(experiments,"analytics_config",lambda:{"benchmark":{
        "shuffle_partitions":2,"warmup_runs":1,"measured_runs":3,
        "relative_tolerance":1e-9,"absolute_tolerance":1e-8}})
    spark.range(100).createOrReplaceTempView("benchmark_fixture")
    sql="SELECT id % 3 AS bucket,count(*) AS n FROM benchmark_fixture GROUP BY id % 3"
    result=experiments.run_pair(spark,"synthetic_cache_test",sql,sql,after_options={"cache":"benchmark_fixture"})
    assert result["results_equal"]
    assert len(result["records"]["before"])==len(result["records"]["after"])==3
    assert [r["order_in_pair"] for r in result["records"]["before"]]==[1,2,1]
    assert (tmp_path/"artifacts/benchmarks/synthetic_cache_test/after.explain_formatted.txt").exists()
